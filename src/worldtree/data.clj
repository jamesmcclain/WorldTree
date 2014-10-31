(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.data
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.memoize :as memo])
  (:import (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream GzipCompressorOutputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

;; Structure to hold $f_{i}(t)$ for some fixed time $t$ and given index $i$
(defstruct function-at-t :f :i)

;; Structure to hold a dataset.  :snapshot is a function that gives
;; the state of the dataset at time t, :n is the number of time
;; series, :m is the number of time steps in each series, and :chunks
;; is the list of chunks.
(defstruct dataset :snapshot :n :m :chunks)

;; Take a file name and return a compressed input stream.
(defmacro fn->cis [filename compstream]
  `(-> ~filename io/file io/input-stream ~compstream io/reader))

;; Compressed output stream.
(defmacro fn->cos [filename]
  `(-> ~filename io/file io/output-stream GzipCompressorOutputStream. io/writer))

(defn dump [directory t thing]
  (spit (str directory "/" t) thing))

;; Load time series data from a row-major data file.
(defn row-major-load [filename extra]
  (let [column-drop (get extra :column-drop 0) ; columns to drop at the beginning (left) of the data set
        column-drop-end (get extra :column-drop-end 0) ; columns to drop at the end (right)
        row-drop (get extra :row-drop 0) ; rows to drop at the beginning (top) of the data set
        file (cond (re-find #"\.gz$" filename) (fn->cis filename GzipCompressorInputStream.) ; gzip
                   (re-find #"\.bz2$" filename) (fn->cis filename BZip2CompressorInputStream.) ; bzip2
                   (re-find #"\.xz$" filename) (fn->cis filename XZCompressorInputStream.) ; xz
                   :else (-> filename io/reader))]
    (with-open [file file]
      (let [lines (drop row-drop (line-seq file))]
        (loop [data [] lines lines]
          (if (empty? lines)
            data     ; nothing more to be read, return list of vectors
            (let [series (string/split (first lines) #"\s+")]  ; split by whitespace
              (if (< (count series) 2)
                (recur data (rest lines)) ; empty line, skip
                (let [series (drop-last column-drop-end (drop column-drop series)) ; drop unwanted columns
                      series (into [] (map #(Double/parseDouble %) series))]
                  (recur (conj data series) (rest lines)))))))))))

;; Return a list of fi-at-t structs for the data at time t.
(defn row-major-timestep [data t]
  (letfn [(series-nth [row index]
            (struct function-at-t
                    (nth row t (last row)) ; :f, $f_{i}(t)$
                    index))]               ; :i, which time series
    (map series-nth data (range))))

;; Load a row-major data file and return a dataset structure.
(defn row-major-dataset [filename & extra]
  (let [extra (apply hash-map extra)    ; extra arguments
        data (row-major-load filename extra)
        snapshot (memo/fifo (fn [t] (row-major-timestep data t)))
        logn (int (Math/ceil (/ (Math/log (count data)) (Math/log 2))))
        chunks (map #(int (Math/pow 2 %)) (range 1 logn))]
    (struct dataset
            snapshot                   ; :snapshot
            (count data)               ; :n, the number of time series
            (reduce #'max (map count data)) ; :m, the number of time steps (same for all series)
            chunks)))
