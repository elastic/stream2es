(ns stream2es.stream.twitter
  (:require [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]])
  (:import (twitter4j.conf ConfigurationBuilder)
           (twitter4j TwitterStreamFactory StatusListener Status)))

(declare make-configuration make-callback)

(def bulk-bytes (* 1024 100))

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
    {})
  (mappings [_ type]
    {(keyword type)
     {:_all {:enabled false}
      :properties {:properties
                   {:location {:type "geo_point"}}}}}))

(extend-type Status
  Streamable
  (make-source [status]
    (let [u (.getUser status)
          place (.getPlace status)
          geo (.getGeoLocation status)]
      (merge
       {:_id (.getId status)
        :created-at (.getCreatedAt status)
        :text (.getText status)
        :user {:id (.getId u)
               :created-at (.getCreatedAt u)
               :name (.getName u)
               :screen-name (.getScreenName u)}}
       (when place
         {:place
          (let [loc (.getGeometryCoordinates place)
                street (.getStreetAddress place)]
            (merge
             {:country (.getCountry place)
              :country-code (.getCountryCode place)
              :url (.getURL place)
              :name (.getFullName place)
              :type (.getPlaceType place)}
             (when loc
               {:location loc})
             (when street
               {:street street})))})
       (when geo
         {:location
          {:lat (.getLatitude geo)
           :lon (.getLongitude geo)}})))))

(defmethod new 'twitter [cmd]
  (TwitterStream.))

(defn make-callback [f]
  (reify StatusListener
    (onStatus [_ status]
      (f status))
    (onStallWarning [_ stall]
      (prn (str stall)))
    (onException [_ e]
      (prn (str e)))
    (onDeletionNotice [_ _])
    (onTrackLimitationNotice [_ _])))

(defn make-configuration [user pass]
  (doto (ConfigurationBuilder.)
    (.setUser user)
    (.setPassword pass)))

(defn mapping []
  )
