(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.segments
  (:require [clojure.math.combinatorics :as combo]
            [clojure.set :as set]
            [clojure.core.reducers :as r]))

;; Line segment structure.
(defstruct segment :m :b :i)

;; An intersection between $f_{i}$ and $f_{j}$ at time $t$.
(defstruct intersection :T :i :j)

;; Compute the segment between $f_{i}(T)$ and $f_{i}(T+1)$.
(defn compute-segment [dataset T i]
  (let [snapshot (:snapshot dataset)
        at-T (:f (nth (snapshot T) i))
        at-T+1 (:f (nth (snapshot (inc T)) i))
        m (- at-T+1 at-T)
        b at-T]
    (struct segment m b i)))

;; Find the point of intersection between two segments.
(defn compute-intersection
  ([[segment1 segment2]]
     (compute-intersection segment1 segment2))
  ([segment1 segment2]
     (let [{m1 :m b1 :b} segment1
           {m2 :m b2 :b} segment2]
       (if (not (== m1 m2))
         (let [t (/ (- b2 b1) (- m1 m2))
               i (:i segment1)
               j (:i segment2)
               [i j] [(min i j) (max i j)]]
           (if (and (<= 0.0 t) (< t 1.0))
             (struct intersection t i j)))))))

;; Compute the series (plural) that might change the composition of
;; the chunk between time T and T+1.
(defn compute-chunk-changers [dataset T chunk]
  (let [topkT (map :i ((:topk dataset) T)) ; top-k at time T
        topkT+1 (map :i ((:topk dataset) (inc T))) ; top-k at time T+1
        before (set (take chunk topkT)) ; composition of the chunk at time T
        after (set (take chunk topkT+1)) ; composition of the chunk at time T+1
        movers (set/difference (set/union before after) (set/intersection before after))]
    (letfn [(relevant? [[i _]] (movers i))]
      (let [relevant-ranks (map second (filter relevant? (map list topkT (range))))
            m (reduce #'min (conj relevant-ranks Long/MAX_VALUE))
            M (inc (reduce #'max (conj relevant-ranks Long/MIN_VALUE)))
            relevant-rank-range (range m M)]
        (map #(nth topkT %) relevant-rank-range)))))

;; Find the pairwise intersections in a bunch segments.
(defn find-intersections-quadratic [segments]
  (remove nil? (map #(compute-intersection %) (combo/combinations segments 2))))

(defn find-intersections [segments]
  (let [n (count segments)]
    (if (< n 333)
                                        ; not many segments, use quadratic algorithm
      (set (find-intersections-quadratic segments))
                                        ; otherwise, use divide-and-conquer algorithm
      (let [intercepts (sort (map :b segments))
            median (nth intercepts (/ n 2))]
        (letfn [(above-median? [segment]
                  (or (<= (:b segment) median)
                      (<= (+ (:b segment) (:m segment)) median)))
                (below-median? [segment]
                  (or (>= (:b segment) median)
                      (>= (+ (:b segment) (:m segment)) median)))
                (through-median? [segment]
                  (let [{b :b m :m} segment
                        b+m (+ b m)]
                    (or (and (<= b median) (<= median b+m))
                        (and (<= b+m median) (<= median b)))))]
          (let [above-seg (filter above-median? segments) ; segments above the median
                above-inter (if (== (count above-seg) n) ; intersections above the median
                              (set (find-intersections-quadratic above-seg))
                              (find-intersections above-seg))
                below-seg (filter below-median? segments) ; segments below the median
                below-inter (if (== (count below-seg) n) ; intersections below the median
                              (set (find-intersections-quadratic below-seg))
                              (find-intersections below-seg))
                through-seg (filter through-median? segments) ; segments and intersections through the median
                through-inter (set (find-intersections-quadratic through-seg))]
            (set/union above-inter through-inter below-inter)))))))

(defn compute-all-intersections [dataset T]
  (letfn [(compute-segment-local [i]
            (compute-segment dataset T i))
          (chunk-to-intersections [chunk]
            (find-intersections (map compute-segment-local (compute-chunk-changers dataset T chunk))))]
    (let [logn (int (Math/ceil (/ (Math/log (:n dataset)) (Math/log 2))))
          chunks (map #(int (Math/pow 2 %)) (range 1 logn))]
      (r/foldcat (r/mapcat chunk-to-intersections chunks)))))
