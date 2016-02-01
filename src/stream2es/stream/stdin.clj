(ns stream2es.stream.stdin
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [stream2es.es :as es]
            [stream2es.http :as http]
            [stream2es.stream :refer [new Stream
                                      Streamable CommandLine
                                      StreamStorage]]))

(defrecord StdinStream [])

(defrecord StdinStreamRunner [runner])

(defmethod new 'stdin [_]
  (StdinStream.))

(extend-type StdinStream
  CommandLine
  (specs [_]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default (* 1024 100)
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue-size" "Size of the internal bulk queue"
      :default 40
      :parse-fn #(Integer/parseInt %)]
     ["--target" "Target ES http://host:port/index/type"
      :default "http://localhost:9200/foo/t"]
     ["--stream-buffer" "Buffer up to this many docs"
      :default 100
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (bootstrap [_ opts]
    {})
  (make-runner [_ opts handler]
    (StdinStreamRunner.
     (fn []
       (loop [in (io/reader *in*)]
         (if-not (.ready *in*)
           (handler :eof)
           (do
             (handler (.readLine *in*))
             (recur in)))))))
  StreamStorage
  (settings [_ opts]
    {:index.number_of_shards 1
     :index.number_of_replicas 0
     :index.refresh_interval -1})
  (mappings [_ opts]
    {(keyword (-> opts :target es/type-name))
     {:_all {:enabled false}
      :properties {}}}))

(extend-type String
  Streamable
  (make-source [doc opts]
    (let [{:keys [_id id] :as document} (json/decode doc true)]
      ;; if an id is available, the capture it
      (if-let [doc-id (or _id id)]
        (merge document {:__s2e_meta__ {:_id doc-id}})
        document))))
