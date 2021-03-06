(ns ^{:author "James McClain <jwm@destroy-data-concepts.com>"}
  worldtree.series
  (:require [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.core.memoize :as memo])
  (:use [gloss.core]
        [gloss.io]))

(defstruct segment :ymin :ymax :m :b :i)
(defstruct node :type :y :left :middle :right)
(defstruct leaf :type :segments)
(defstruct intersection :T+t :i :j)
(defstruct change :T+t :ij)
(defstruct timestep :chunk-list :change-list)

(def change-frame (compile-frame (struct change :float64 [:uint16 :uint16]))) ; n ≤ 2^16 - 1
(def timestep-frame (compile-frame (struct timestep (repeated :uint16) (repeated change-frame))))

;; ------------------------- SEGMENTS AND RANKINGS -------------------------

;; Comparator for segments
(defn segment< [one two]
  (let [{m1 :m b1 :b i1 :i} one
        {m2 :m b2 :b i2 :i} two]
    (cond (and (== b1 b2) (== m1 m2)) (< i1 i2)
          (== b1 b2) (> m1 m2)
          :else (> b1 b2))))

(defmacro snapshot
  ([dataset t]
     `((get ~dataset :snapshot) ~t))
  ([dataset t i]
     `(nth ((get ~dataset :snapshot) ~t) ~i)))

;; Compute all of the segments at the given time.
(defn compute-segments [dataset time]
  (letfn [(compute-segment [dataset ^long t ^long i]
            (let [y0 (snapshot dataset t i)
                  y1 (snapshot dataset (inc t) i)
                  m (- y1 y0)
                  b y0
                  b+m (+ b m)]
              (struct segment (min b b+m) (max b b+m) m b i)))]
    (map #(compute-segment dataset time %) (range (dec (:n dataset))))))

;; ------------------------- SEGMENT TREE -------------------------

;; Take a sorted list of segments and return a segment tree suitable
;; for finding overlapping segments.
(defn build-segment-tree [segments]
  (if (not (empty? segments))
    (let [median (rand-nth segments)
          y (/ (+ (:ymin median) (:ymax median)) 2)]
      (letfn [(left?  [segment] (< (:ymax segment) y))
              (right? [segment] (> (:ymin segment) y))]
        (let [[left middle right]
              (loop [left [] middle [] right [] segments segments]
                (if (empty? segments)
                  [left middle right]   ; if done, return the lists
                  (let [segment (first segments)
                        segments (rest segments)]
                    (cond (left? segment) (recur (conj left segment) middle right segments)
                          (right? segment) (recur left middle (conj right segment) segments)
                          :else (recur left (conj middle segment) right segments)))))]
          ;; At this point, left, middle, and right are bound to the
          ;; vectors computed in the loop.
          (if (and (empty? left) (empty? right))
            (struct leaf :leaf segments)
            (struct node :node y (build-segment-tree left) (build-segment-tree middle) (build-segment-tree right))))))))

;; Query the segment tree for segments that intersect q after time
;; T+t.  Only the first such segment is returned.
(defn- query-segment-tree [T tree q T+t]
  (letfn [(compute-intersection [T segment1 segment2]
            (let [{m1 :m b1 :b} segment1
                  {m2 :m b2 :b} segment2]
              (if (not (== m1 m2))
                (let [t (/ (- b2 b1) (- m1 m2))
                      i (:i segment1)
                      j (:i segment2)
                      [i j] [(min i j) (max i j)]]
                  (if (and (< 0.0 t) (< t 1.0))
                    (struct intersection (+ t T) i j))))))
          (after-time? [inter] ; after time T+t?
            (if (not (nil? inter)) (> (:T+t inter) T+t)))]
    (cond (= (:type tree) :node) ; node
          (let [y (:y tree)
                ymin (:ymin q)
                ymax (:ymax q)
                inters (list (query-segment-tree T (:middle tree) q T+t)
                             (if (<= ymin y) (query-segment-tree T (:left tree) q T+t))
                             (if (>= ymax y) (query-segment-tree T (:right tree) q T+t)))
                inters (remove nil? inters)]
            (if (not (empty? inters))
              (reduce (partial min-key :T+t) inters)))
          (= (:type tree) :leaf) ; leaf
          (let [inters (map #(compute-intersection T q %) (:segments tree))
                inters (filter after-time? inters)]
            (if (not (empty? inters))
              (reduce (partial min-key :T+t) inters))))))

;; ------------------------- CHUNKS AND TIMESTEPS -------------------------

;; Find all of the intersections that change the composition of the
;; chunk for times between (T,T+1).
(defn- compute-chunk-intersections [tick segments tree starting-segment]
  (loop [current-segment starting-segment
         current-time (+ tick 0.0)
         trace []]
    (let [inter (query-segment-tree tick tree current-segment current-time)]
      (if (nil? inter)
        trace ; no more intersections in this timestep, return trace
        (let [next-index (if (== (:i inter) (:i current-segment))
                           (:j inter) (:i inter)) ; the index of the intersecting segment
              next-segment (nth segments next-index) ; the intersecting segment
              next-time (:T+t inter)
              change (struct change next-time (list (:i current-segment) next-index))]
          (if (not (segment< next-segment current-segment))
            ;; If next-segment has a worse rank than the
            ;; current-segment, then the former's intersection with
            ;; the latter changes the chunk's composition, so that
            ;; intersection must be recorded.
            (recur next-segment next-time (conj trace change))
            ;; Otherwise, if next-segment has a better rank, that
            ;; means that it is intersecting the current segment from
            ;; a superior position.  The fact that next-segment is the
            ;; new bottom of the chunk needs to be remembered, but the
            ;; intersection itself does not.
            (recur next-segment next-time trace)))))))

;; Find all of the action in [T,T+1) and record it.
(defn compute-and-store-timestep [dir chunks tick segments tree sorted-a]
  (doseq [chunk chunks]
    (let [chunk-list (map :i (take chunk sorted-a))
          change-list (compute-chunk-intersections tick segments tree (nth sorted-a (dec chunk)))
          timestep (struct timestep chunk-list change-list)
          filename (str dir "/" chunk "/" tick)]
      (with-open [out (io/output-stream filename)]
        (encode-to-stream timestep-frame out (list timestep))))))

;; Fetch timestep [T,T+1).
(defn fetch-timestep [dir chunk tick]
  (let [file (io/file (str dir "/" chunk "/" tick))]
    (with-open [in (io/input-stream file)]
      (let [buffer (byte-array (.length file))]
        (.read in buffer)
        (let [step (decode timestep-frame buffer)]
          (struct timestep (set (:chunk-list step)) (:change-list step)))))))

(def fetch-timestep-memo (memo/fifo fetch-timestep :fifo/threshold (* 12 1024)))
