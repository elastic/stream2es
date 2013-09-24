(ns stream2es.stream.twitter
  (:require [cheshire.core :as json]
            [stream2es.auth :as auth]
            [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]
            [stream2es.util.data :refer [maybe-update-in remove-in-if]]
            [stream2es.util.time :as time])
  (:import (twitter4j.conf ConfigurationBuilder)
           (twitter4j TwitterStreamFactory RawStreamListener FilterQuery)
           (twitter4j.json DataObjectFactory)))

(declare make-configuration make-callback correct-polygon single-point?)

(def bulk-bytes (* 1024 100))

(def weird-twitter-date-format
  "EEE MMM dd HH:mm:ss Z yyyy")

(def date-format
  (str weird-twitter-date-format "||yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

(def locale
  "en_EN")

(defrecord Status [json])

(defrecord TwitterStream [])

(defrecord TwitterStreamRunner [runner])

(extend-type TwitterStream
  CommandLine
  (specs [this]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default bulk-bytes
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue-size" "Size of the internal bulk queue"
      :default 1000
      :parse-fn #(Integer/parseInt %)]
     ["-i" "--index" "ES index" :default "twitter"]
     ["-t" "--type" "ES document type" :default "status"]
     ["--authorize" "Create oauth credentials" :flag true :default false]
     ["--track" "Comma-separated list of phrases to determine what Tweets will be delivered on the stream."]
     ["--key" "Twitter app consumer key, only for --authorize"]
     ["--secret" "Twitter app consumer secret, only for --authorize"]
     ["--stream-buffer" "Buffer up to this many tweets"
      :default 1000
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (make-runner [this opts handler]
    (let [authinfo (:authinfo opts)
          filter (FilterQuery. )
          conf (.build (make-configuration authinfo))
          stream (doto (-> (TwitterStreamFactory. conf) .getInstance)
                   (.addListener (make-callback handler)))]
      (when (:track opts)
        (.track filter (into-array (clojure.string/split (:track opts) #",+"))))
      (if (:track opts)
        (TwitterStreamRunner. #(.filter stream filter))
        (TwitterStreamRunner. #(.sample stream)))))

  StreamStorage
  (settings [_]
    {:query.default_field :text
     :index.refresh_interval "5s"
     :index {:analysis
             {:analyzer
              {:fulltext_analyzer
               {:type :custom
                :tokenizer :whitespace
                :filter [:lowercase, :bigram]}}
              :filter
              {:bigram
               {:type :shingle
                :max_shingle_size 2
                :min_shingle_size 2
                :output_unigrams true
                :output_unigrams_if_no_shingles true
                :token_separator " "}}}}})
  (mappings [_ type]
    {(keyword type)
     {:_all {:enabled false}
      :_size {:enabled true
              :store "yes"}
      :dynamic_date_formats [weird-twitter-date-format
                             "date_time"
                             "date_optional_time"]
      :properties
      {:created_at {:type :date
                    :format date-format
                    :locale locale}
       :user
       {:properties
        {:created_at {:type :date
                      :format date-format
                      :locale locale}}}

       :retweeted_status
       {:properties
        {:created_at {:type :date
                      :format date-format
                      :locale locale}
         :user
         {:properties
          {:created_at {:type :date
                        :format date-format
                        :locale locale}}}}}
       :entities
       {:properties
        {:hashtags
         {:properties
          {:text {:type :string
                  :index :not_analyzed}}}}}
       :coordinates
       {:properties
        {:coordinates {:type "geo_point"}}}
       :place
       {:dynamic true
        :properties
        {:bounding_box {:type "geo_shape"}
         :country {:type :multi_field
                   :fields {:country
                            {:type :string
                             :index :analyzed}
                            :keyword
                            {:type :string
                             :index :not_analyzed}}}
         :country_code {:type :multi_field
                        :fields {:country_code
                                 {:type :string
                                  :index :analyzed}
                                 :keyword
                                 {:type :string
                                  :index :not_analyzed}}}}}}}}))

(extend-type Status
  Streamable
  (make-source [status]
    (let [status* (json/decode (:json status) true)]
      (when (:id status*)
        (-> (dissoc status* :id)
            (assoc :_id (:id status*))
            (remove-in-if [:place :bounding_box] nil?)
            (remove-in-if [:place :bounding_box] single-point?)
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

(defn make-configuration [authinfo]
  (let [creds (auth/get-current-creds authinfo :twitter)]
    (doto (ConfigurationBuilder.)
      (.setOAuthConsumerKey (:key creds))
      (.setOAuthConsumerSecret (:secret creds))
      (.setOAuthAccessToken (:token creds))
      (.setOAuthAccessTokenSecret (:token-secret creds))
      (.setJSONStoreEnabled true))))

(defn correct-polygon [polys?]
  (map (fn [poly]
         (conj poly (first poly)))
       polys?))

(defn single-point? [box]
  (= 1 (count (into #{} (:coordinates box)))))

(defn oauth-consumer [opts]
  (auth/make-oauth-consumer
   (:key opts)
   (:secret opts)
   "http://api.twitter.com/oauth/request_token"
   "http://api.twitter.com/oauth/access_token"
   "http://api.twitter.com/oauth/authorize"
   :hmac-sha1))

(defn make-creds [opts]
  (let [tok (auth/get-token! (oauth-consumer opts))]
    {:type :twitter
     :created (time/now)
     :token (:oauth_token tok)
     :token-secret (:oauth_token_secret tok)
     :key (:key opts)
     :secret (:secret opts)}))
