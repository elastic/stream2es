(ns stream2es.stream.es
  (:require [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.es :as es]
            [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]))

(declare make-callback search-context-missing-explanation)

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
     ["--source" "Source ES url"]
     ["--target" "Target ES url"]
     ["--query" "Query to _scan from source" :default match-all]
     ["--scroll-size" "Source scroll size"
      :default 500
      :parse-fn #(Integer/parseInt %)]
     ["--scroll-time" "Source scroll context TTL" :default "60s"]])
  Stream
  (bootstrap [_ opts]
    {})
  (make-runner [this opts handler]
    (ElasticsearchStreamRunner. (make-callback opts handler)))
  StreamStorage
  (settings [_ opts]
    (es/settings (:source opts)))
  (mappings [_ opts]
    (es/mapping (:source opts))))

(extend-type Document
  Streamable
  (make-source [doc opts]
    (:m doc)))

(defmethod new 'es [cmd]
  (ElasticsearchStream.))

(defn make-doc [hit]
  (->Document
   (merge (:_source hit)
          {:__s2e_meta__
           (merge
            {:_id (:_id hit)
             :_type (:_type hit)}
            (when (get-in hit [:fields :_routing])
              {:_routing (get-in hit [:fields :_routing])})
            (when (get-in hit [:fields :_parent])
              {:_parent (get-in hit [:fields :_parent])}))})))

(defn make-callback [opts handler]
  (fn []
    (try+
     (doseq [hit (es/scan (:source opts)
                          (:query opts)
                          (:scroll-time opts)
                          (:scroll-size opts))]
       (-> hit make-doc handler))
     (handler :eof)
     (catch [:type :stream2es.es/search-context-missing] _
       (throw+ {:type :stream-death
                :msg search-context-missing-explanation})))))

(def search-context-missing-explanation
  "

The search scroll is closing before stream2es is able to return and
get another batch of hits. This typically means that ES is under
pressure on one side or the other.

Try either increasing --scroll-time or decreasing --scroll-size.

")
