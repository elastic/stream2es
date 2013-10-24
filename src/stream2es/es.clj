(ns stream2es.es
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [stream2es.log :as log]))

(defn base-url [full]
  (let [u (java.net.URL. full)]
    (format "%s://%s:%s"
            (.getProtocol u)
            (.getHost u)
            (.getPort u))))

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

(defn scroll*
  "One set of hits mid-scroll."
  [url id ttl]
  (let [resp (http/get
              (format "%s/_search/scroll" url)
              {:body id
               :query-params {:scroll ttl}})]
    (json/decode (:body resp) true)))

(defn scroll
  "lazy-seq of hits from on originating scroll_id."
  [url id ttl]
  (let [resp (scroll* url id ttl)
        hits (-> resp :hits :hits)
        new-id (:_scroll_id resp)]
    (lazy-seq
     (when (seq hits)
       (cons (first hits) (concat (rest hits) (scroll url new-id ttl)))))))

(defn scan1
  "Set up scroll context."
  [url query ttl size]
  (let [resp (http/get
              (format "%s/_search" url)
              {:body query
               :query-params
               {:search_type "scan"
                :scroll ttl
                :size size}})]
    (json/decode (:body resp) true)))

(defn scan
  "Client entry point. Returns a scrolling lazy-seq of hits."
  [url query ttl size]
  (let [resp (scan1 url query ttl size)]
    (scroll (base-url url) (:_scroll_id resp) ttl)))
