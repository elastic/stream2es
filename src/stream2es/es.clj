(ns stream2es.es
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [stream2es.log :as log]))

(defn index-url [url index]
  (format "%s/%s" url index))

(defn post
  ([url data]
     (log/trace "POSTing" (count (.getBytes data)) "bytes")
     (http/post url {:body data}))
  ([url index data]
     (http/post (index-url url index) {:body data})))

(defn delete [url index]
  (http/delete (index-url url index) {:throw-exceptions false}))

(defn exists? [url index]
  (try
    (http/get (format "%s/_mapping" (index-url url index)))
    (catch Exception _)))

(defn error-capturing-bulk [url items serialize-bulk]
  (let [resp (json/decode (:body (post url (serialize-bulk items))) true)]
    (->> (:items resp)
         (map-indexed (fn [n obj]
                        (when (contains? (val (first obj)) :error)
                          (spit (str "error-"
                                     (:_id (val (first obj))))
                                (with-out-str
                                  (prn obj)
                                  (println)
                                  (prn (nth items n))))
                          obj)))
         (remove nil?)
         count)))
