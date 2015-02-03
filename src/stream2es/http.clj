(ns stream2es.http
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as http])
  (:import (java.net URL)))

(defn put [url opts]
  (http/put url opts))

(defn post [url opts]
  (http/post url opts))

(defn delete [url opts]
  (http/delete url opts))

(defn get [url opts]
  (http/get url opts))

(defprotocol HttpUrl
  (jurl [this])
  (base-url [this]))

(defn components [url]
  (let [u (java.net.URL. url)]
    {:proto (.getProtocol u)
     :host (.getHost u)
     :port (.getPort u)
     :user-info (.getUserInfo u)}))

(deftype Target [url components opts]
  HttpUrl
  (jurl [this]
    (let [u (.url this)]
      (when u
        (cond
          (.startsWith u "http") (URL. u)
          :else (URL. (str "file://" u))))))
  (base-url [this]
    (let [{:keys [proto user-info host port]} (.components this)]
      (format "%s://%s%s:%s"
              proto
              (if user-info
                (str user-info "@")
                "")
              host port))))

(defn make-target
  ([url]
   (Target. url (components url) {}))
  ([url http-opts]
   (Target. url (components url) http-opts)))
