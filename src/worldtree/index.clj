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
        (dorun (pmap (partial series/compute-and-store-frame dir chunks)
                     times
                     segment-lists
                     segment-trees
                     sorted-lists
                     (concat (next sorted-lists) (list (series/compute-rankings dataset (dec (dec m))))))))
      (println (java.util.Date.)))))

;; Query a search index.
(defn query [dir k t]
  (let [frame (read-string (slurp (str dir "/" k "/" (int t))))]
    (loop [topk (set (:chunk-list frame)) changes (:change-list frame)]
      (let [change (first changes)]
        (if (or (nil? change) (>= (:T+t change) t))
          ;; no more changes and/or past query time, return topk
          (do
            (assert (== (count topk) k))
            topk)
          ;; update the topk and iterate again
          (let [[out in] (:ij change)]
            (recur (conj (disj topk out) in) (rest changes))))))))

;; Do n random queries.  This function may only work on Linux (it
;; certainly requires a /proc filesystem).
(defn random-queries [dir k t n]
  (letfn [(get-io [] ; read information out of /proc/$pid/io
            (let [pid (first (string/split (.getName (java.lang.management.ManagementFactory/getRuntimeMXBean)) #"@"))
                  [rchar _ syscr _ _ _ _] (map #(Double/parseDouble %) (re-seq #"\d+" (slurp (str "/proc/" pid "/io"))))]
              [rchar syscr]))
          (get-millis [] ; current time in milliseconds
            (double (.getTimeInMillis (java.util.Calendar/getInstance))))
          (get-vitals [] ; concatenate proc stuff and millis together
            (conj (get-io) (get-millis)))]
    (let [before (get-vitals)]
      (dotimes [_ n]
        (query dir k (rand (dec t))))
      (let [after (get-vitals)]
        ; rchars per query, syscr per query, milliseconds per query
        (map #(/ % n) (map - after before))))))
