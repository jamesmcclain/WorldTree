(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.index
  (:require [worldtree.series :as series]))

;; Build a search index for the given dataset in the given directory.
(defn build-index [dataset dir]
  (letfn [(is-dir? [thing] (.isDirectory (java.io.File. thing)))
          (exists? [thing] (.exists (java.io.File. thing)))]
    (let [{m :m n :n chunks :chunks} dataset
          subdir (map #(str dir "/" %) (:chunks dataset))]
                                        ; check/make directories
      (assert (= (filter is-dir? subdir) (filter exists? subdir))
              "There is at least one non-directory with the same name as a necessary directory.")
      (doseq [dir (remove is-dir? subdir)] (.mkdirs (java.io.File. dir)))
                                        ; compute
      (println "Segment Lists" "\t\t" (java.util.Date.))
      (let [times (range (dec m))
            segment-lists (doall (pmap (partial series/compute-segments dataset) times))]
        (println "Sorted Segment Lists" "\t" (java.util.Date.))
        (let [sorted-lists (doall (pmap #(sort series/segment< %) segment-lists))]
          (println "Segment Trees" "\t\t" (java.util.Date.))
          (let [segment-trees (doall (pmap series/build-segment-tree segment-lists))]
            (println "Building Index" "\t\t" (java.util.Date.))
            (dorun (pmap (partial series/compute-and-store-frame dir chunks)
                         times
                         segment-lists
                         segment-trees
                         sorted-lists
                         (next sorted-lists))))))
      (println "Finished." "\t\t" (java.util.Date.)))))
