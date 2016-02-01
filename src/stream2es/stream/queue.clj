(ns stream2es.stream.queue
  (:require [cheshire.core :as json]
            [clojure.java.io :as jio]
            [elastiqueue.core :as q]
            [stream2es.es :as es]
            [stream2es.util.io :as io]
            [stream2es.log :as log]
            [stream2es.stream :refer [new Stream
                                      Streamable CommandLine
                                      StreamStorage]]))

(defrecord QueueStream [])

(defrecord QueueStreamRunner [runner])

(defmethod new 'queue [_]
  (QueueStream.))

(extend-type QueueStream
  CommandLine
  (specs [_]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default (* 1024 100)
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue-size" "Size of the internal bulk queue"
      :default 40
      :parse-fn #(Integer/parseInt %)]
     ["--target" "ES index" :default "http://localhost:9200/foo/t"]
     ["--stream-buffer" "Buffer up to this many docs"
      :default 100
      :parse-fn #(Integer/parseInt %)]
     ["--broker" "Broker url"]
     ["--exchange" "Broker exchange"]
     ["--queue" "Broker queue"]])
  Stream
  (bootstrap [_ opts]
    {})
  (make-runner [_ opts handler]
    (QueueStreamRunner.
     (fn []
       (let [e (q/declare-exchange (:broker opts) (:exchange opts))
             q (q/declare-queue e (:queue opts))]
         (log/debug 'consume-poll (:broker opts) (:exchange opts) (:queue opts))
         (q/consume-poll q (fn [msg]
                             (if-let [src (-> msg :_source :source)]
                               (try
                                 (log/debug 'start src)
                                 (doall
                                  (map handler
                                       (line-seq
                                        (io/gz-reader src))))
                                 (q/ack msg)
                                 (log/debug 'finish src)
                                 (catch Exception e
                                   (log/debug e)))
                               (log/debug 'consume-exception
                                          "no source in msg"
                                          msg))))))))
  StreamStorage
  (settings [_ opts]
    {:number_of_shards 2
     :number_of_replicas 0})
  (mappings [_ opts]
    {(keyword (-> opts :target es/components :type))
     {:_all {:enabled false}
      :properties {}}}))
