(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.series
  (:require [clojure.set :as set]
            [clojure.core.reducers :as r]
            [clojure.core.memoize :as memo]))

(defstruct segment :ymin :ymax :m :b :i)
(defstruct node :type :y :left :middle :right)
(defstruct leaf :type :segments)
(defstruct intersection :T+t :i :j)
(defstruct change :T+t :ij)
(defstruct frame :chunk-list :change-list)

;; Compute the segment between $f_{i}(t)$ and $f_{i}(t+1)$.
(defn compute-segment [dataset t i]
  (let [snapshot (:snapshot dataset)
        y0 (:f (nth (snapshot t) i))
        y1 (:f (nth (snapshot (inc t)) i))
        m (- y1 y0)
        b y0
        b+m (+ b m)]
    (struct segment (min b b+m) (max b b+m) m b i)))

;; Comparator for segments
(defn segment< [one two]
  (let [{m1 :m b1 :b i1 :i} one
        {m2 :m b2 :b i2 :i} two]
    (cond (and (== b1 b2) (== m1 m2)) (< i1 i2)
          (== b1 b2) (> m1 m2)
          :else (> b1 b2))))

;; Compute all of the segments at time T.
(def compute-all-segments
  (memo/fifo
   (fn [dataset T] (map #(compute-segment dataset T %) (range (dec (:n dataset)))))))

;; Segments at time T sorted by rank.
(def compute-sorted-segments
  (memo/fifo
   (fn [dataset T] (sort segment< (compute-all-segments dataset T)))))

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
                ;; NOTE: the fact that the lists are vectors means
                ;; that the sorted order will be preserved when lists
                ;; are built up with conj.
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

;; Find the point of intersection between two segments (the time of
;; intersection of the segments).
(defn compute-intersection [T segment1 segment2]
  (let [{m1 :m b1 :b} segment1
        {m2 :m b2 :b} segment2]
    (if (not (== m1 m2))
      (let [t (/ (- b2 b1) (- m1 m2))
            i (:i segment1)
            j (:i segment2)
            [i j] [(min i j) (max i j)]]
        (if (and (< 0.0 t) (< t 1.0))
          (struct intersection (+ t T) i j))))))

;; Query the segment tree for segments that intersect q after time
;; T+t.  Only the first such segment is returned.
(defn query-segment-tree [T tree q T+t]
  (letfn [(before? [intersection] ; does the intersection before time T+t?
            (if (not (nil? intersection))
              (<= (:T+t intersection) T+t)))]
    (cond
                                        ; if this is a node, query the subtrees
     (= (:type tree) :node)
     (let [y (:y tree)
           ymin (:ymin q)
           ymax (:ymax q)]
       (or (if (<= ymin y) (query-segment-tree T (:left tree) q T+t))
           (query-segment-tree T (:middle tree) q T+t)
           (if (>= ymax y) (query-segment-tree T (:right tree) q T+t))))
                                        ; if this is a leaf, find the first intersection after T+t
     (= (:type tree) :leaf)
     (first (remove before? (sort-by :T+t (map #(compute-intersection T q %) (:segments tree)))))
                                        ; otherwise, do nothing
     :else nil)))

;; Find all of the intersections that change the composition of the
;; chunk for times between (T,T+1).
(defn chunk-intersection-changes [T tree segments starting-segment]
  (loop [current-segment starting-segment
         current-time (+ T 0.0)
         trace []]
    (let [intersection (query-segment-tree T tree current-segment current-time)]
      (if (nil? intersection)
        trace ; no more intersections in this frame, return trace
        (let [next-index (if (== (:i intersection) (:i current-segment))
                           (:j intersection) (:i intersection)) ; the index of the intersecting segment
              next-segment (nth segments next-index) ; the intersecting segment
              next-time (:T+t intersection)
              change (struct change next-time (list (:i current-segment) next-index))]
          (if (segment< next-segment current-segment)
                                        ; if next-segment has better rank, chunk does not change.
            (recur next-segment next-time trace)
                                        ; otherwise, it does
            (recur next-segment next-time (conj trace change))))))))

;; Find all of the intersections that change the composition of the
;; chunk for all times between (T,T+1].  This is done by using
;; chunk-intersection-changes, then tracing through from T+0 using
;; those intersections.  After that is done, the difference between
;; what the intersections do and the chunk at time T+1 is found and
;; recorded.
(defn chunk-all-changes [T tree segments sortedT+0 sortedT+1 chunk]
  (let [trace (chunk-intersection-changes T tree segments (nth sortedT+0 (dec chunk)))
        A (set (take chunk (map :i sortedT+0))) ; the set of series in the chunk at time T
        B (loop [current A trace trace] ; the chunk just before T+1
            (if (empty? trace)
              current
              (let [change (first trace)
                    trace (rest trace)
                    out (first (:ij change))
                    in (second (:ij change))]
                (if (not (current in))
                                        ; remove old member and add new one
                  (recur (conj (disj current out) in) trace)
                                        ; otherwise, no change
                  (recur current trace)))))
        C (set (take chunk (map :i sortedT+1))) ; the chunk at time T+1
        B-C (set/difference B C)
        C-B (set/difference C B)
        enders (map #(struct change (+ T 1.0) (list %1 %2)) B-C C-B)]
    (concat trace enders)))

;; Record all of the changes of rank in the dataset that are relevant
;; vis-a-vis chunks.
(defn record-all-changes [dataset dir]
  (letfn [(is-dir? [thing] (.isDirectory (java.io.File. thing)))
          (exists? [thing] (.exists (java.io.File. thing)))]
    (let [m (:m dataset)
          subdir (map #(str dir "/" %) (:chunks dataset))]
      (assert (= (filter is-dir? subdir) (filter exists? subdir))
              "There is at least one non-directory with the same name as a necessary directory.")
      (doseq [dir (remove is-dir? subdir)] (.mkdirs (java.io.File. dir))) ; make directories
      (doseq [T (range (dec m))] ; for every time
        (let [segments (compute-all-segments dataset T) ; segments at time T sorted by index
              sortedT+0 (compute-sorted-segments dataset T) ; segments at time T+0 sorted by rank
              sortedT+1 (compute-sorted-segments dataset (inc T)) ; segments at time T+1 sorted by rank
              tree (build-segment-tree (vec segments))]
          (if (== 0 (mod T 107))
            (println (int (/ T m 0.01)) "% \t" (java.util.Date.)))
          (doseq [chunk (:chunks dataset)] ; for every chunk size
            (let [chunk-list (set (map :i (take chunk sortedT+0))) ; the set of series in this chunk
                  change-list (chunk-all-changes T tree segments sortedT+0 sortedT+1 chunk)]
              (spit (str dir "/" chunk "/" T)
                    (struct frame chunk-list change-list))))))
      (println "Finished."))))
