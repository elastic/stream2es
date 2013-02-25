(ns stream2es.version
  (:require [clojure.java.io :as io]))

(defn version []
  (-> "version.txt" io/resource slurp .trim))
