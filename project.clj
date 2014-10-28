(defproject worldtree "0.33-SNAPSHOT"
  :description "A program/library for doing Top-k queries on time series."
  :url "http://daystrom-data-concepts.com/TOPK/"
  :license {:name "The BSD 3-Clause License"
            :url "http://opensource.org/licenses/BSD-3-Clause"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha2"]
                 [clojure-complete "0.2.4"]
                 [org.clojure/tools.nrepl "0.2.3"]
                 [cider/cider-nrepl "0.8.0-SNAPSHOT"]
                 [org.apache.commons/commons-compress "1.4.1"]
                 [org.clojure/core.memoize "0.5.6"]]
  :source-paths ["src"]
  :main worldtree.core
  :aot [worldtree.core])
