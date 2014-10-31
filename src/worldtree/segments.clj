(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.segments
  (:require [clojure.math.combinatorics :as combo]
            [clojure.set :as set]
            [clojure.core.reducers :as r]))

;; Line segment structure.
(defstruct segment :yrange :m :b :i)

;; Compute the segment between $f_{i}(T)$ and $f_{i}(T+1)$.
(defn compute-segment [dataset T i]
  (let [snapshot (:snapshot dataset)
        at-T (:f (nth (snapshot T) i))
        at-T+1 (:f (nth (snapshot (inc T)) i))
        m (- at-T+1 at-T)
        b at-T
        b+m (+ b m)]
    (struct segment [(min b b+m) (max b b+m)] m b i)))

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
