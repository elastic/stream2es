(ns stream2es.es
  (:refer-clojure :exclude [meta])
  (:require [cheshire.core :as json]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.http :as http]
            [stream2es.log :as log]
            [stream2es.util.typed :refer [unnullable]])
  (:import (stream2es.http Target)))

(defprotocol EsUrl
  (index-url [this])
  (mapping-url [this])
  (bulk-url [this])
  (search-url [this])
  (scroll-url [this]))

(defprotocol IndexManager
  (delete-index [this])
  (put-index [this body])
  (index-exists? [this])
  (index-name [this])
  (type-name [this])
  (meta [this type])
  (mapping [this])
  (settings [this]))

(defn components [url]
  (let [u (java.net.URL. url)
        [_ index type id] (re-find
                           #"/*([^/]+)/?([^/]+)?/?([^/]+)?"
                           (.getPath u))]
    {:proto (.getProtocol u)
     :host (.getHost u)
     :port (.getPort u)
     :index index
     :type type
     :id id
     :user-info (.getUserInfo u)}))

(defn put
  ([url opts]
   (log/trace "PUT" (if (:body opts)
                      (count (.getBytes (:body opts)))
                      0)
              "bytes")
   (http/put url opts)))

(defn post
  ([url opts]
   (log/trace "POST" (if (:body opts)
                       (count (.getBytes (:body opts)))
                       0)
              "bytes")
   (http/post url opts)))

(defn delete [url]
  (log/info "delete index" url)
  (http/delete url {:throw-exceptions false}))

(defn error-capturing-bulk [target items serialize-bulk]
  (let [resp (json/decode (:body (post (bulk-url target)
                                       (assoc (.opts target)
                                              :body (serialize-bulk items)))) true)]
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
  [target id ttl]
  (try+
   (let [resp (http/get
               (scroll-url target)
               (merge {:body id
                       :query-params {:scroll ttl}}
                      (.opts target)))]
     (json/decode (:body resp) true))
   (catch Object {:keys [body]}
     (cond
       (re-find #"SearchContextMissingException" body)
       (throw+ {:type ::search-context-missing})
       :else (throw+ {:type ::wat})))))

(defn scroll
  "lazy-seq of hits from on originating scroll_id."
  [target id ttl]
  (let [resp (scroll* target id ttl)
        hits (-> resp :hits :hits)
        new-id (:_scroll_id resp)]
    (lazy-seq
     (when (seq hits)
       (cons (first hits) (concat (rest hits) (scroll target new-id ttl)))))))

(defn scan1
  "Set up scroll context."
  [target query ttl size]
  (let [resp (http/get
              (search-url target)
              (merge {:body query
                      :query-params
                      {:search_type "scan"
                       :scroll ttl
                       :size size
                       :fields "_source,_routing,_parent"}}
                     (.opts target)))]
    (json/decode (:body resp) true)))

(defn scan
  "Client entry point. Returns a scrolling lazy-seq of hits."
  [target query ttl size]
  (let [resp (scan1 target query ttl size)]
    (scroll target (:_scroll_id resp) ttl)))

(extend-type Target
  EsUrl
  (index-url [this]
    (format "%s/%s" (http/base-url this)
            (unnullable ::index-name (index-name this))))
  (bulk-url [this]
    (format "%s/%s" (.url this) "_bulk"))
  (search-url [this]
    (format "%s/_search" (.url this)))
  (scroll-url [this]
    (format "%s/_search/scroll" (http/base-url this)))


  IndexManager
  (index-name [this]
    (:index (.components this)))
  (type-name [this]
    (:type (.components this)))
  (delete-index [this]
    (delete (index-url this)))
  (index-exists? [this]
    (try
      (http/get (format "%s/_mapping" (index-url this)) (.opts this))
      (catch Exception _)))
  (put-index [this body]
    (http/put (index-url this) (assoc (.opts this)
                                      :body body)))
  (meta [this suffix]
    (let [resp (-> (http/get (format "%s/%s" (index-url this) suffix)
                             (merge {:throw-exceptions? true} (.opts this)))
                   :body
                   (json/decode true))
          index (index-name this)]
      (-> (resp (keyword index)) first val)))

  (mapping [this]
    (meta this "_mapping"))
  (settings [this]
    (meta this "_settings")))

(defn make-target
  ([url]
   (make-target url (components url) {}))
  ([url http-opts]
   (Target. url (http/make-jurl url) (components url) http-opts)))
