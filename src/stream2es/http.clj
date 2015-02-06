(ns stream2es.http
  (:refer-clojure :exclude [get defprotocol])
  (:require [clojure.core.typed
             :refer [ann ann-datatype defprotocol Any Map Kw Num U IFn]
             :as t])
  (:require [clj-http.client :as http])
  (:import (java.net URL)))

(ann ^:no-check clj-http.client/put [String (Map Kw Any) -> (Map Kw Any)])
(ann ^:no-check clj-http.client/post [String (Map Kw Any) -> (Map Kw Any)])
(ann ^:no-check clj-http.client/delete [String (Map Kw Any) -> (Map Kw Any)])
(ann ^:no-check clj-http.client/get [String (Map Kw Any) -> (Map Kw Any)])

(ann put [String (Map Kw Any) -> (Map Kw Any)])
(defn put [url opts]
  (http/put url opts))

(ann post [String (Map Kw Any) -> (Map Kw Any)])
(defn post [url opts]
  (http/post url opts))

(ann delete [String (Map Kw Any) -> (Map Kw Any)])
(defn delete [url opts]
  (http/delete url opts))

(ann get [String (Map Kw Any) -> (Map Kw Any)])
(defn get [url opts]
  (http/get url opts))

(ann components [String -> (HMap :mandatory {:proto (U String nil)
                                             :host (U String nil)
                                             :port Num
                                             :user-info (U String nil)})])
(defn components [url]
  (let [u (java.net.URL. url)]
    {:proto (.getProtocol u)
     :host (.getHost u)
     :port (.getPort u)
     :user-info (.getUserInfo u)}))

(defprotocol HttpUrl
  (jurl [this] :- URL)
  (base-url [this] :- String))

(ann-datatype Target [url :- String
                      components :- (HMap :mandatory {:proto (U String nil)
                                                      :host (U String nil)
                                                      :port Num
                                                      :user-info (U String nil)})
                      opts :- (Map Kw Any)])
(deftype Target [url components opts]
  HttpUrl
  (jurl [this]
    (let [^String u (.url this)]
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

(ann make-target
     (IFn [String -> Target]
          [String (Map Kw Any) -> Target]))
(defn make-target
  ([url]
   (Target. url (components url) {}))
  ([url http-opts]
   (Target. url (components url) http-opts)))
