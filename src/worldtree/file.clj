(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.file
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

(defn row-major [filename & extra]
  (letfn [(xz? [filename] (re-find #"\.xz$" filename))]
    (let [extra (apply hash-map extra)
          column-drop-initial (get extra :column-drop-initial (get extra :column-drop 0))
          column-drop-final (get extra :column-drop-final 0)
          row-drop (get extra :row-drop 0)
          file (if (xz? filename)
                 (-> filename io/file io/input-stream XZCompressorInputStream. io/reader)
                 (-> filename io/reader))
          lines (drop row-drop (line-seq file))]
      (loop [data '() lines lines]
        (if (empty? lines)
          data
          (let [series (string/split (first lines) #"\s+")
                series (drop-last column-drop-final (drop column-drop-initial series))
                series (into [] (map #(Double/parseDouble %) series))]
            (recur (conj data series) (rest lines))))))))
