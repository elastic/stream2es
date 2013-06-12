(ns stream2es.stream.twitter
  (:require [cheshire.core :as json]
            [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]
            [stream2es.util.data :refer [maybe-update-in]])
  (:import (twitter4j.conf ConfigurationBuilder)
           (twitter4j TwitterStreamFactory RawStreamListener)
           (twitter4j.json DataObjectFactory)))

(declare make-configuration make-callback correct-polygon)

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
     ["--key" "Twitter consumer key" :default ""]
     ["--secret" "Twitter consumer secret" :default ""]
     ["--token" "Twitter token" :default ""]
     ["--token-secret" "Twitter token secret" :default ""]
     ["--stream-buffer" "Buffer up to this many tweets"
      :default 1000
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (make-runner [this {:keys [key secret token token-secret]} handler]
    (let [conf (.build (make-configuration key secret token token-secret))
          stream (doto (-> (TwitterStreamFactory. conf) .getInstance)
                   (.addListener (make-callback handler)))]
      (TwitterStreamRunner. #(.sample stream))))
  StreamStorage
  (settings [_]
    {:query.default_field :text})
  (mappings [_ type]
    {(keyword type)
     {:_all {:enabled false}
      :dynamic_date_formats ["EEE MMM dd HH:mm:ss Z yyyy"
                             "dateOptionalTime"]
      :properties
      {:entities
       {:properties
        {:hashtags
         {:properties
          {:text {:type :string
                  :index :not_analyzed}}}}}
       :coordinates
       {:properties
        {:coordinates {:type "geo_point"}}}
       :place
       {:properties
        {:bounding_box {:type "geo_shape"}}}}}}))

(extend-type Status
  Streamable
  (make-source [status]
    (let [status* (json/decode (:json status) true)]
      (when (:id status*)
        (-> (dissoc status* :id)
            (assoc :_id (:id status*))
            (maybe-update-in [:place :bounding_box :coordinates]
                             correct-polygon))))))

(defmethod new 'twitter [cmd]
  (TwitterStream.))

(defn make-callback [f]
  (reify RawStreamListener
    (onMessage [_ json]
      (f (->Status json)))
    (onException [_ e]
      (prn (str e)))))

(defn make-configuration [consumer_key, consumer_secret, access_token, access_token_secret]
  (doto (ConfigurationBuilder.)
    (.setOAuthConsumerKey consumer_key)
    (.setOAuthConsumerSecret consumer_secret)
    (.setOAuthAccessToken access_token)
    (.setOAuthAccessTokenSecret access_token_secret)
    (.setJSONStoreEnabled true)))

(defn correct-polygon [polys?]
  (map (fn [poly]
         (conj poly (first poly)))
       polys?))
