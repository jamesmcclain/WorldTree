(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.index
  (:require  [clojure.string :as string]
             [worldtree.series :as series]))

;; Build a search index for the given dataset in the given directory.
(defn build [dataset dir]
  (letfn [(is-dir? [thing] (.isDirectory (java.io.File. thing)))
          (exists? [thing] (.exists (java.io.File. thing)))]
    (let [{m :m n :n chunks :chunks} dataset
          subdir (map #(str dir "/" %) (:chunks dataset))]
                                        ; check/make directories
      (assert (= (filter is-dir? subdir) (filter exists? subdir))
              "There is at least one non-directory with the same name as a necessary directory.")
      (doseq [dir (remove is-dir? subdir)] (.mkdirs (java.io.File. dir)))
                                        ; compute
      (println (java.util.Date.) " ... ")
      (let [times (range (dec m))
            segment-lists (doall (pmap (partial series/compute-segments dataset) times))
            sorted-lists (pmap #(sort series/segment< %) segment-lists)
            segment-trees (pmap series/build-segment-tree segment-lists)]
        (dorun (pmap (partial series/compute-and-store-timestep dir chunks)
                     times
                     segment-lists
                     segment-trees
                     sorted-lists)))
      (println (java.util.Date.)))))

;; Query a search index.
(defn query [dir k t]
  (let [timestep (series/fetch-timestep-memo dir k (int t))]
    (loop [topk (:chunk-list timestep) changes (:change-list timestep)]
      (let [change (first changes)]
        (if (or (nil? change) (>= (:T+t change) t))
          topk ; no more changes and/or past query time, return topk
          (let [[out in] (:ij change)] ; update the topk and iterate again
            (if (and (not (topk in)) (topk out))
              (recur (conj (disj topk out) in) (rest changes))
              (recur topk (rest changes)))))))))

;; Do n random queries.  This function may only work on Linux (it
;; certainly requires a /proc filesystem and for /proc/self/io to
;; exist [the latter is not the case on Joyent Ubuntu 12.04 virtual
;; machines, for example]).
(defn random-queries [dir k t n]
  (letfn [(get-io [] ; read information out of /proc/$pid/io
            (let [[rchar _ syscr _ _ _ _] (map #(Double/parseDouble %) (re-seq #"\d+" (slurp (str "/proc/self/io"))))]
              [rchar syscr]))
          (get-millis [] ; current time in milliseconds
            (double (.getTimeInMillis (java.util.Calendar/getInstance))))
          (get-vitals [] ; concatenate proc stuff and millis together
            (conj (get-io) (get-millis)))]
    (let [before (get-vitals)]
      (dotimes [_ n]
        (query dir k (rand (dec t))))
      (let [after (get-vitals)]
        ;; characters read per query, read system calls per query, milliseconds per query
        (map #(/ % n) (map - after before))))))
