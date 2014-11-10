(ns ^{:author "James McClain <jwm@daystrom-data-concepts.com>"}
  worldtree.data
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.memoize :as memo])
  (:import (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream GzipCompressorOutputStream)
           (org.apache.commons.compress.compressors.bzip2 BZip2CompressorInputStream)
           (org.apache.commons.compress.compressors.xz XZCompressorInputStream)))

(defstruct dataset :series :snapshot :n :m :chunks)

;; Write the dataset in the format that the program buildSEB.exe
;; expects.  Care must be taken to produce a file that that program
;; can use (very frustrating) ...
(defn seb-write [dataset filename]
  (with-open [w (java.io.FileWriter. filename)]
    (binding [*out* w]
      (letfn [(rand+ [x] (+ x (rand 0.0001)))
              (write-xy [x y] (println x (rand+ y)))]
        (println (:n dataset)) ; write the number of series
        (dotimes [i (:n dataset)]
          (let [series ((get dataset :series) i)
                m (count series)
                bizarre-magic-number (/ 10000000.0 (dec m))
                x-ticks (range)
                x-ticks (map #(* % bizarre-magic-number) x-ticks)
                x-ticks (map int x-ticks)]
            (println m) ; write the length of this series
            (dorun (map write-xy x-ticks series))))))))

;; Down-sample data.  Might be useful for getting datasets to work
;; with buildSEB.exe.
(defn downsample-data [data n]
  (letfn [(average [nums] (/ (reduce #'+ nums) n))
          (downsample-series [series] (map average (partition n series)))]
    (vec (doall (map downsample-series data)))))

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

;; Produce a collection of rows of random data.
(defn random-data [n m sigma]
  (letfn [(random-series [mean]
            (doall (into (vector-of :double)
                         (repeatedly m (fn [] (NormalRandomVariable mean sigma))))))]
    (vec (doall (map random-series (range 0 (* 10 n) 10))))))

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

;; Turn a sequence of data rows into a dataset struct.
(defn row-major-dataset [data]
  (let [n (count data)
        m (reduce #'max (map count data))
        series (fn [i] (nth data i))
        snapshot (memo/fifo (fn [t] (doall (into (vector-of :double) (map #(nth % t) data)))) :fifo/threshold (inc m))
        logn (int (Math/ceil (/ (Math/log (count data)) (Math/log 2))))
        chunks (map #(int (Math/pow 2 %)) (range 1 logn))]
    (struct dataset
            series snapshot
            n m chunks)))
