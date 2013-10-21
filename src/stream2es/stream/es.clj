(ns stream2es.stream.es
  (:require [cheshire.core :as json]
            [stream2es.es :as es]
            [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]))

(declare make-callback)

(def match-all
  "{\"query\":{\"match_all\":{}}}")

(def bulk-bytes
  (* 1024 1024 1))

(defrecord Document [m])

(defrecord ElasticsearchStream [])

(defrecord ElasticsearchStreamRunner [runner])

(extend-type ElasticsearchStream
  CommandLine
  (specs [this]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default bulk-bytes
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue-size" "Size of the internal bulk queue"
      :default 1000
      :parse-fn #(Integer/parseInt %)]
     ["-i" "--index" "Target ES index"]
     ["-t" "--type" "Target ES type"]
     ["--source" "Source ES url"]
     ["--query" "Query to _scan from source" :default match-all]
     ["--scroll-size" "Source scroll size"
      :default 500
      :parse-fn #(Integer/parseInt %)]
     ["--scroll-time" "Source scroll context TTL" :default "15s"]])
  Stream
  (make-runner [this opts handler]
    (ElasticsearchStreamRunner. (make-callback opts handler)))
  StreamStorage
  (settings [_]
    {:index.refresh_interval "5s"})
  (mappings [_ type] {}))

(extend-type Document
  Streamable
  (make-source [doc]
    (:m doc)))

(defmethod new 'es [cmd]
  (ElasticsearchStream.))

(defn make-doc [hit]
  (->Document
   (merge (:_source hit)
          {:_id (:_id hit)})))

(defn make-callback [opts handler]
  (fn []
    (doall
     (map handler
          (concat
           (map make-doc
                (es/scan (:source opts)
                         (:query opts)
                         (:scroll-time opts)
                         (:scroll-size opts)))
           [:eof])))))
