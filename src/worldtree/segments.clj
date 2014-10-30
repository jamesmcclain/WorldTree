(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.segments
  (:require [clojure.math.combinatorics :as combo]
            [clojure.set :as set]
            [clojure.core.reducers :as r]))

;; Line segment structure.
(defstruct segment :m :b :i)

;; An intersection between $f_{i}$ and $f_{j}$ at time $t$.
(defstruct intersection :chunk :T :i :j)

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
  ([T chunk [segment1 segment2]]
     (compute-intersection T chunk segment1 segment2))
  ([T chunk segment1 segment2]
     (let [{m1 :m b1 :b} segment1
           {m2 :m b2 :b} segment2]
       (if (not (== m1 m2))
         (let [t (/ (- b2 b1) (- m1 m2))
               i (:i segment1)
               j (:i segment2)
               [i j] [(min i j) (max i j)]]
           (if (and (<= 0.0 t) (< t 1.0))
             (struct intersection chunk (+ T t) i j)))))))

;; Compute the series (plural) that might change the composition of
;; the chunk between time T and T+1.
(defn compute-chunk-changers [dataset T chunk]
  (let [topkT (map :i ((:topk dataset) T)) ; top-k at time T
        topkT+1 (map :i ((:topk dataset) (inc T))) ; top-k at time T+1
        before (set (take chunk topkT)) ; composition of the chunk at time T
        after (set (take chunk topkT+1)) ; composition of the chunk at time T+1
        arrivals (set/difference after before)
        departures (set/difference before after)
        relevant (set/union arrivals departures)]
    (letfn [(relevant? [[i _]] (relevant i))]
      (let [relevant-ranks (map second (filter relevant? (map list topkT (range))))
            m (reduce #'min relevant-ranks)
            M (reduce #'max relevant-ranks)
            relevant-rank-range (range m (inc M))]
        (map #(nth topkT %) relevant-rank-range)))))

;; Find the pairwise intersections in a bunch segments.
(defn find-intersections-quadratic [T chunk segments]
  (remove nil? (map (partial compute-intersection T chunk) (combo/combinations segments 2))))

;; (defn find-intersections [segments]
;;   (let [n (count segments)]
;;     (if (< n 333)
;;                                         ; not many segments, use quadratic algorithm
;;       (set (find-intersections-quadratic segments))
;;                                         ; otherwise, use divide-and-conquer algorithm
;;       (let [intercepts (sort (map :b segments))
;;             median (nth intercepts (/ n 2))]
;;         (letfn [(above-median? [segment]
;;                   (or (<= (:b segment) median)
;;                       (<= (+ (:b segment) (:m segment)) median)))
;;                 (below-median? [segment]
;;                   (or (>= (:b segment) median)
;;                       (>= (+ (:b segment) (:m segment)) median)))
;;                 (through-median? [segment]
;;                   (let [{b :b m :m} segment
;;                         b+m (+ b m)]
;;                     (or (and (<= b median) (<= median b+m))
;;                         (and (<= b+m median) (<= median b)))))]
;;           (let [above (filter above-median? segments) ; segments above the median
;;                 above (if (== (count above) n) ; intersections above the median
;;                         (set (find-intersections-quadratic above))
;;                         (find-intersections above))

;;                 below (filter below-median? segments) ; segments below the median
;;                 below (if (== (count below) n) ; intersections below the median
;;                         (set (find-intersections-quadratic below))
;;                         (find-intersections below))

;;                 through (filter through-median? segments) ; segments and intersections through the median
;;                 through (set (find-intersections-quadratic through))]
;;             (set/union above through below)))))))
