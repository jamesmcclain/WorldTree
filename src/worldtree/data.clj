(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.data
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.memoize :as memo])
  (:import (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

;; Structure to hold $f_{i}(t)$ for some fixed time $t$ and given index $i$
(defstruct fi-at-t :f :i)

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
(defn row-major-timestep [data t]
  (letfn [(series-nth [row index]
            (struct fi-at-t (nth row t (last row)) index))]
    (sort-by :f (map series-nth data (range)))))

;; Load a row-major data file and return (i) a function that gives a
;; sorted list of time series, (ii) the number of time series, and
;; (iii) the number of time steps.
(defn row-major-dataset [filename & extra]
  (let [extra (apply hash-map extra) ; extra arguments
        data (row-major-load filename extra)]
    {:fn (memo/fifo (partial row-major-timestep data) :fifo/threshold 2)
     :i (count data)
     :t (reduce #'max (map count data))}))
