(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.data
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.memoize :as memo])
  (:import (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream GzipCompressorOutputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

(defstruct function-at-t :f :i)
(defstruct dataset :series :snapshot :n :m :chunks)

;; Take a file name and return a compressed input stream.
(defmacro fn->cis [filename compstream]
  `(-> ~filename io/file io/input-stream ~compstream io/reader))

;; Compressed output stream.
(defmacro fn->cos [filename]
  `(-> ~filename io/file io/output-stream GzipCompressorOutputStream. io/writer))

(defmacro fnkey [dataset fun & args]
  (cond
   (= (count args) 1) `((get ~dataset ~fun) ~(first args))
   (= (count args) 2) `(nth ((get ~dataset ~fun) ~(first args)) ~(second args))
   :else `nil))

;; ------------------------- ROW MAJOR DATA -------------------------

;; Load time series data from a row-major data file.
(defn row-major-load [filename & extra]
  (let [extra (apply hash-map extra)    ; extra arguments
        column-drop (get extra :column-drop 0) ; columns to drop at the beginning (left) of the data set
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

;; Return a list of function-at-t structs for the data at time t.
(defn- row-major-timestep [data t]
  (letfn [(series-nth [row index]
            (struct function-at-t
                    (nth row t (last row)) ; :f, $f_{i}(t)$
                    index))]               ; :i, which time series
    (map series-nth data (range))))

;; Load a row-major data file and return a dataset structure.
(defn row-major-dataset [data]
  (let [snapshot (memo/fifo (fn [t] (row-major-timestep data t)))
        logn (int (Math/ceil (/ (Math/log (count data)) (Math/log 2))))
        chunks (map #(int (Math/pow 2 %)) (range 1 logn))]
    (struct dataset
            #(nth data %)              ; series
            snapshot                   ; :snapshot
            (count data)               ; :n, the number of time series
            (reduce #'max (map count data)) ; :m, the number of time steps (same for all series)
            chunks)))

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
