(ns stream2es.util.io
  (:require [clojure.java.io :as io])
  (:import (java.util.zip GZIPOutputStream)
           (java.net URL)))

(def file io/file)

(defn spit-gz
  "Opens f with gzipped writer, writes content, then
  closes f. Options passed to clojure.java.io/writer."
  [f content & options]
  (let [gz (-> f io/output-stream GZIPOutputStream.)]
    (with-open [#^java.io.Writer w (apply io/writer gz options)]
      (.write w (str content)))))
