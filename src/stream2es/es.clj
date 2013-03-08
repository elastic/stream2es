(ns stream2es.es
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [clojure.tools.logging :as log]))

(defn index-url [url index]
  (format "%s/%s" url index))

(defn post
  ([url data]
     (log/trace "POSTing" (count (.getBytes data)) "bytes")
     (http/post url {:body data}))
  ([url index data]
     (http/post (index-url url index) {:body data})))

(defn delete [url index]
  (http/delete (index-url url index)))

(defn exists? [url index]
  (try
    (http/get (format "%s/_mapping" (index-url url index)))
    (catch Exception _)))
