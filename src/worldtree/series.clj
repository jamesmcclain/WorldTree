(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.series
  (:require [clojure.set :as set]))

(defstruct segment :ymin :ymax :m :b :i)
(defstruct node :type :y :left :middle :right)
(defstruct leaf :type :segments)
(defstruct intersection :T+t :i :j)
(defstruct change :T+t :ij)
(defstruct frame :chunk-list :change-list)

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

;; Compute the rankings at thus-and-so time.
(defn compute-rankings [dataset time]
  (map #(hash-map :i %) (sort-by first (map list (snapshot dataset time) (range)))))

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

;; ------------------------- CHUNKS AND FRAMES -------------------------

;; Find all of the intersections that change the composition of the
;; chunk for times between (T,T+1).
(defn- compute-chunk-intersections [T tree segments starting-segment]
  (loop [current-segment starting-segment
         current-time (+ T 0.0)
         trace []]
    (let [inter (query-segment-tree T tree current-segment current-time)]
      (if (nil? inter)
        trace ; no more intersections in this frame, return trace
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

;; Find all of the intersections that change the composition of the
;; chunk for all times between (T,T+1].  First use
;; compute-chunk-intersections, then look at the difference between
;; what that does and the composition at time T+1.
(defn- compute-chunk-changes [tick segments tree sorted-a sorted-b chunk]
  (let [trace (compute-chunk-intersections tick tree segments (nth sorted-a (dec chunk)))
        A (set (take chunk (map :i sorted-a))) ; the set of series in the chunk at the start time
        B (loop [current A trace trace] ; the chunk just before the end time
            (if (empty? trace)
              current
              (let [change (first trace)
                    trace (rest trace)
                    out (first (:ij change))
                    in (second (:ij change))]
                (if (not (current in))
                  ;; If the new series "in" is not already in chunk,
                  ;; then it does need to be added.
                  (recur (conj (disj current out) in) trace)
                  ;; If "in" is already in the chunk, then there is no
                  ;; need to do a swap.
                  (recur current trace)))))
        C (set (take chunk (map :i sorted-b))) ; the chunk at the end time
        B-C (set/difference B C)
        C-B (set/difference C B)
        enders (map #(struct change (+ tick 1.0) (list %1 %2)) B-C C-B)]
    (concat trace enders)))

;; Find all of the action in (T,T+1] and record it.
(defn compute-and-store-frame [dir chunks tick segments tree sorted-a sorted-b]
  (doseq [chunk chunks]
    (let [chunk-list (map :i (take chunk sorted-a))
          change-list (compute-chunk-changes tick segments tree sorted-a sorted-b chunk)]
      (spit (str dir "/" chunk "/" tick) (struct frame chunk-list change-list)))))
