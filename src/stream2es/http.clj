(ns stream2es.http
  (:refer-clojure :exclude [get defprotocol])
  (:require [clojure.core.typed
             :refer [ann ann-datatype defprotocol Any Map Kw Num U IFn]
             :as t])
  (:require [clj-http.client :as http]
            [stream2es.util.typed :refer [throw+]])
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

(ann components [URL -> (HMap :mandatory {:proto (U String nil)
                                          :host (U String nil)
                                          :port Num
                                          :user-info (U String nil)})])
(defn components [^URL u]
  {:proto (.getProtocol u)
   :host (.getHost u)
   :port (.getPort u)
   :user-info (.getUserInfo u)})

(ann make-jurl [String -> URL])
(defn make-jurl [^String u]
  (when u
    (cond
      (.startsWith u "http") (URL. u)
      (.startsWith u "file:") (URL. u)
      (.startsWith u "/") (URL. (str "file://" u))
      :else (throw+ {:type ::urlparse}))))

(defprotocol HttpUrl
  (base-url [this] :- String))

(ann-datatype Target [url :- String
                      jurl :- URL
                      components :- (HMap :mandatory {:proto (U String nil)
                                                      :host (U String nil)
                                                      :port Num
                                                      :user-info (U String nil)})
                      opts :- (Map Kw Any)])
(deftype Target [url jurl components opts]
  HttpUrl
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
   (make-target url {}))
  ([url http-opts]
   (let [j (make-jurl url)]
     (Target. url j (components j) http-opts))))
