(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.intersections
  (:require [clojure.math.combinatorics :as combo]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [worldtree.segments :as segments]))

;; An intersection between $f_{i}$ and $f_{j}$ at time $t$.
(defstruct intersection :T :i :j)

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

;; Find the pairwise intersections in a bunch segments.
(defn compute-intersections-quadratic [segments]
  (remove nil? (map #(compute-intersection %) (combo/combinations segments 2))))

;; Find the pairwise intersections in a large bunch of segments.
(defn compute-intersections-nlgn [segments]
  (let [n (count segments)]
    (if (< n 300)
                                        ; not many segments, use quadratic algorithm
      (set (compute-intersections-quadratic segments))
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
                              (set (compute-intersections-quadratic above-seg))
                              (compute-intersections above-seg))
                below-seg (filter below-median? segments) ; segments below
                below-inter (if (== (count below-seg) n) ; intersections below
                              (set (compute-intersections-quadratic below-seg))
                              (compute-intersections below-seg))
                through-seg (filter through-median? segments)
                through-inter (set (compute-intersections-quadratic through-seg))]
            (set/union above-inter through-inter below-inter)))))))

(defn report-all-intersections [dataset T]
  (compute-intersections-nlgn
   (r/foldcat
    (r/map (partial segments/compute-segment dataset T) (range (:n dataset))))))

(defn report-interesting-intersections [dataset T]
  (compute-intersections-nlgn
   (r/foldcat
    (r/map (partial segments/compute-segment dataset T)
           (r/mapcat (partial segments/compute-chunk-changers dataset T) (:chunks dataset))))))
