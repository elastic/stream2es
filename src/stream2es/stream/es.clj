(ns stream2es.stream.es
  (:require [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.util.data :refer [re-replace-keys]]
            [stream2es.es :as es]
            [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]))

(declare
 make-callback
 search-context-missing-explanation
 validate!)

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
     ["--scroll-time" "Source scroll context TTL" :default "60s"]
     ["--source-http-insecure" "Don't verify peer cert" :flag true :default false]
     ["--source-http-keystore" "/path/to/keystore"]
     ["--source-http-keystore-pass" "Keystore password"]
     ["--source-http-trust-store" "/path/to/keystore"]
     ["--source-http-trust-store-pass" "Truststore password"]
     ])
  Stream
  (bootstrap [_ opts]
    (validate! opts (:source opts) (:target opts))
    (let [just-http (re-replace-keys opts #"^source-http-")]
      {:source (es/make-target (:source opts) just-http)}))
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
            {:_type (:_type hit)
             :_id (:_id hit)}
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

(defn validate! [opts source target]
  (let [s* (es/components source)
        t* (.components target)
        spath [(:host s*) (:port s*)]
        tpath [(:host t*) (:port t*)]]
    (cond
      (and (:indexing opts)
           (= spath tpath)
           (not (:index t*)))
      (throw+ {:type :stream-invalid-args
               :stream 'es
               :msg "must provide an index on the target cluster"})

      )))
