(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.data
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.memoize :as memo])
  (:import (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream GzipCompressorOutputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

(defstruct dataset :series :snapshot :n :m :chunks)

;; ------------------------- ROW MAJOR DATA -------------------------

;; Load time series data from a row-major data file.
(defn row-major-load [filename & extra]
  (let [extra (apply hash-map extra)    ; extra arguments
        column-drop (get extra :column-drop 0) ; columns to drop at the beginning (left) of the data set
        column-drop-end (get extra :column-drop-end 0) ; columns to drop at the end (right)
        row-drop (get extra :row-drop 0) ; rows to drop at the beginning (top) of the data set
        file (cond (re-find #"\.gz$" filename) (-> filename io/file io/input-stream GzipCompressorInputStream. io/reader)
                   (re-find #"\.bz2$" filename) (-> filename io/file io/input-stream BZip2CompressorInputStream. io/reader)
                   (re-find #"\.xz$" filename) (-> filename io/file io/input-stream XZCompressorInputStream. io/reader)
                   :else (-> filename io/reader))]
    (with-open [file file]
      (let [lines (drop row-drop (line-seq file))]
        (loop [data [] lines lines]
          (if (empty? lines)
            data ; nothing more to be read, return list of vectors
            (let [series (string/split (first lines) #"\s+")]  ; split by whitespace
              (if (< (count series) 2)
                (recur data (rest lines)) ; empty line, skip
                (let [series (drop-last column-drop-end (drop column-drop series)) ; drop unwanted columns
                      series (into (vector-of :double) (map #(Double/parseDouble %) series))]
                  (recur (conj data series) (rest lines)))))))))))

;; Load a row-major data file and return a dataset structure.
(defn row-major-dataset [data]
  (let [n (count data)
        m (reduce #'max (map count data))
        series (fn [i] (nth data i))
        snapshot (memo/fifo (fn [t] (into (vector-of :double) (map #(nth % t) data))) :fifo/threshold (inc m))
        logn (int (Math/ceil (/ (Math/log (count data)) (Math/log 2))))
        chunks (map #(int (Math/pow 2 %)) (range 1 logn))]
    (struct dataset
            series snapshot
            n m chunks)))

;; ------------------------- RANDOM DATA -------------------------

(def ^:const TWOPI 6.2831853071795864769252867665590057683943387987502116)

;; https://en.wikipedia.org/wiki/Box_Muller_transform
(defn- NormalRandomVariable [mean stdev]
  (let [U1 (rand)
        U2 (rand)]
    (+ mean
       (* stdev
          (Math/sqrt (* -2 (Math/log U1)))
          (Math/cos (* TWOPI U2))))))

(defn random-data [n m sigma]
  (letfn [(random-series [mean]
            (into (vector-of :double)
                  (repeatedly m (fn [] (NormalRandomVariable mean sigma)))))]
    (vec (map random-series (range 0 (* 10 n) 10)))))
