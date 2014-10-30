(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.data
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.memoize :as memo])
  (:import (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

;; Structure to hold $f_{i}(T)$ for some fixed time $t$ and given index $i$
(defstruct f-at-T :f :i)

;; Structure to hold a dataset.  :snapshot is a function that gives
;; the state of the dataset at time t.  :topk is a function that gives
;; the sorted order of the series (plural) indices.
(defstruct dataset :snapshot :topk :n :max_t)

;; Take a file name and return a compressed input stream.
(defmacro fn->cis [filename compstream]
  `(-> ~filename io/file io/input-stream ~compstream io/reader))

;; Load time series data from a row-major data file.
(defn row-major-load [filename extra]
  (let [column-drop (get extra :column-drop 0) ; columns to drop at the beginning (left) of the data set
        column-drop-end (get extra :column-drop-end 0) ; columns to drop at the end (right)
        row-drop (get extra :row-drop 0) ; rows to drop at the beginning (top) of the data set
        file (cond (re-find #"\.gz$" filename) (fn->cis filename GzipCompressorInputStream.) ; gzip
                   (re-find #"\.bz2$" filename) (fn->cis filename BZip2CompressorInputStream.) ; bzip2
                   (re-find #"\.xz$" filename) (fn->cis filename XZCompressorInputStream.) ; xz
                   :else (-> filename io/reader)) ; plain
        lines (drop row-drop (line-seq file))]
    (loop [data '() lines lines]
      (if (empty? lines)
        data ; nothing more to be read, return list of vectors
        (let [series (string/split (first lines) #"\s+") ; split by whitespace
              series (drop-last column-drop-end (drop column-drop series)) ; drop unwanted columns
              series (into [] (map #(Double/parseDouble %) series))]
          (recur (conj data series) (rest lines)))))))

;; Return a list of fi-at-t structs for the data at time t.
(defn row-major-timestep [data T]
  (letfn [(series-nth [row index]
            (struct f-at-T
                    (nth row T (last row)) ; :f, $f_{i}(T)$
                    index))] ; :i, which time series
    (map series-nth data (range))))

;; Load a row-major data file and return (i) a function that gives a
;; sorted list of time series, (ii) the number of time series, and
;; (iii) the number of time steps.
(defn row-major-dataset [filename & extra]
  (let [extra (apply hash-map extra) ; extra arguments
        data (row-major-load filename extra)
        snapshot (memo/fifo (partial row-major-timestep data))
        topk (memo/fifo (fn [t] (sort-by :f (snapshot t))))]
    (struct dataset
            snapshot ; :snapshot
            topk ; :topk
            (count data) ; :n, the number of time series
            (reduce #'max (map count data))))) ; :max_t the largest time index
