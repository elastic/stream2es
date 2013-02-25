(ns wiki2es.twitter
  (:import (twitter4j.conf ConfigurationBuilder)
           (twitter4j TwitterStreamFactory StatusListener)))

(def bulk-bytes
  (* 1024 100))

(def opts
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

(defn make-source [status]
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
         :lon (.getLongitude geo)}}))))

(defn make-stream [user pass handler]
  (let [conf (.build (make-configuration user pass))
        stream (doto (-> (TwitterStreamFactory. conf) .getInstance)
                 (.addListener (make-callback handler)))]
    {:run #(.sample (:stream %))
     :stream stream}))
