(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.segments
  (:use [worldtree.data :only [fi-at-t]]))

;; Line segment structure.
(defstruct segment :m :b :i)

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
