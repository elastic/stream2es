(ns stream2es.stream.twitter
  (:require [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]
            [cheshire.core :as json])
  (:import (twitter4j.conf ConfigurationBuilder)
           (twitter4j TwitterStreamFactory RawStreamListener)
           (twitter4j.json DataObjectFactory)))

(declare make-configuration make-callback)

(def bulk-bytes (* 1024 100))

(defrecord Status [json])

(defrecord TwitterStream [])

(defrecord TwitterStreamRunner [runner])

(extend-type TwitterStream
  CommandLine
  (specs [this]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default bulk-bytes
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue" "Size of the internal bulk queue"
      :default 1000
      :parse-fn #(Integer/parseInt %)]
     ["-i" "--index" "ES index" :default "twitter"]
     ["-t" "--type" "ES document type" :default "status"]
     ["--user" "Twitter username" :default ""]
     ["--pass" "Twitter password" :default ""]
     ["--stream-buffer" "Buffer up to this many tweets"
      :default 1000
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (make-runner [this {:keys [user pass]} handler]
    (let [conf (.build (make-configuration user pass))
          stream (doto (-> (TwitterStreamFactory. conf) .getInstance)
                   (.addListener (make-callback handler)))]
      (TwitterStreamRunner. #(.sample stream))))
  StreamStorage
  (settings [_]
    {:query.default_field :text})
  (mappings [_ type]
    {(keyword type)
     {:_all {:enabled false}
      :dynamic_date_formats ["EEE MMM dd HH:mm:ss Z yyyy"]
      :properties {:location {:type "geo_point"}}}}))

(extend-type Status
  Streamable
  (make-source [status]
    (let [status* (json/decode (:json status) true)]
      (when (:id status*)
        (assoc (dissoc status* :id)
          :_id (:id status*))))))

(defmethod new 'twitter [cmd]
  (TwitterStream.))

(defn make-callback [f]
  (reify RawStreamListener
    (onMessage [_ json]
      (f (->Status json)))
    (onException [_ e]
      (prn (str e)))))

(defn make-configuration [user pass]
  (doto (ConfigurationBuilder.)
    (.setUser user)
    (.setPassword pass)
    (.setJSONStoreEnabled true)))
