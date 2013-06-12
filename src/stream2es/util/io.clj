(ns stream2es.util.io
  (:require [clojure.java.io :as io])
  (:import (java.util.zip GZIPOutputStream)
           (java.net URL)
           (java.nio.file.attribute PosixFilePermission)
           (java.nio.file FileSystems Files)))

(def file io/file)

(defn spit-gz
  "Opens f with gzipped writer, writes content, then
  closes f. Options passed to clojure.java.io/writer."
  [f content & options]
  (let [gz (-> f io/output-stream GZIPOutputStream.)]
    (with-open [#^java.io.Writer w (apply io/writer gz options)]
      (.write w (str content)))))

(defn get-path [one & more]
  (.getPath (FileSystems/getDefault) one (into-array String more)))

(defn chmod-0600 [filename]
  (let [path (get-path filename)]
    (Files/setPosixFilePermissions
     path
     #{PosixFilePermission/OWNER_READ
       PosixFilePermission/OWNER_WRITE})))
