(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.segments
  (:require [clojure.math.combinatorics :as combo]
            [clojure.set :as set]
            [clojure.core.reducers :as r]))

;; Line segment structure.
(defstruct segment :m :b :i)

;; An intersection between $f_{i}$ and $f_{j}$ at time $t$.
(defstruct intersection :t :i :j)

;; A function to return all of the segments between from
;; $f_{i}(t_{1})$ to $f_{i}(t_{2})$ for all $i$ in the dataset.
(defn compute-segments [dataset t1 t2]
  (let [timestep (:fn dataset) ; a function that returns the value of every series at time t
        at-time-t1 (timestep t1) ; the series (plural) at time t1
        at-time-t2 (timestep t2)] ; the same at time t2
    (letfn [(compute-segment [at-time-t1 at-time-t2]
              (struct segment
                      (- (:f at-time-t2) (:f at-time-t1)) ; :m, the slope
                      (:f at-time-t1) ; :b, the y-intercept
                      (:i at-time-t1)))] ; :i, the series (singular) that this segment is from
      (map compute-segment at-time-t1 at-time-t2))))

;; Find the point of intersection between two segments.
(defn compute-intersection
  ([[segment1 segment2]] (compute-intersection segment1 segment2))
  ([segment1 segment2]
     (let [{m1 :m b1 :b} segment1
           {m2 :m b2 :b} segment2]
       (if (not (== m1 m2))
         (let [t (/ (- b2 b1) (- m1 m2))
               i (min (:i segment1) (:i segment2))
               j (max (:i segment1) (:i segment2))]
           (if (and (<= 0.0 t) (< t 1.0))
             (struct intersection t i j)))))))

;; Find the pairwise intersections in a bunch segments.
(defn find-intersections-quadratic [segments]
  (r/foldcat
   (r/remove nil?
    (r/map compute-intersection (combo/combinations segments 2)))))

(defn find-intersections [segments]
  (let [n (count segments)]
    (if (< n 200)
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
          (let [through (filter through-median? segments)
                above (filter above-median? segments)
                below (filter below-median? segments)]
            (println (count above) (count through) (count below))
            (set/union
                                        ; above
             (if (== (count above) n)
               (set (find-intersections-quadratic above))
               (find-intersections above))
                                        ; through
             (if (== (count through) n)
               (set (find-intersections-quadratic through))
               (find-intersections through))
                                        ; below
             (if (== (count below) n)
               (set (find-intersections-quadratic below))
               (find-intersections below)))))))))
