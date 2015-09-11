(ns stream2es.stream.wiki
  (:require [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]]
            [stream2es.http :as http])
  (:import (org.elasticsearch.river.wikipedia.support
            PageCallbackHandler WikiPage
            WikiXMLParserFactory)
           (stream2es.http Target)))

(declare make-parser make-callback)

(def bulk-bytes (* 3 1024 1024))

(def latest-wiki
  (str "https://dumps.wikimedia.org"
       "/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"))

(defrecord WikiStream [])

(defrecord WikiStreamRunner [runner])

(defmethod new 'wiki [cmd]
  (WikiStream.))

(extend-type WikiStream
  CommandLine
  (specs [this]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default bulk-bytes
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue-size" "Size of the internal bulk queue"
      :default 40
      :parse-fn #(Integer/parseInt %)]
     ["--target" "Target ES http://host:port/index (we handle types here)"
      :default "http://localhost:9200/wiki"]
     ["--source" "Wiki dump location" :default latest-wiki]
     ["--stream-buffer" "Buffer up to this many pages"
      :default 50
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (bootstrap [_ opts]
    {:source (http/make-target (:source opts))})
  (make-runner [this opts handler]
    (let [stream (make-parser (:source opts) handler)]
      (WikiStreamRunner. #(.parse stream))))
  StreamStorage
  (settings [_ _]
    {:index.number_of_shards 2
     :index.number_of_replicas 0
     :index.refresh_interval :5s
     :query.default_field :text})
  (mappings [_ _]
    {:_default_ {:_all {:enabled false}
                 :_size {:enabled true}}}))

(extend-type WikiPage
  Streamable
  (make-source [page opts]
    {:__s2e_meta__ {:_id (.getID page)
                    :_type (cond
                             (.isDisambiguationPage page) :disambiguation
                             (.isRedirect page) :redirect
                             :else :page)}
     :title (-> page .getTitle str .trim)
     :text (-> (.getText page) .trim)
     :redirect (.isRedirect page)
     :special (.isSpecialPage page)
     :stub (.isStub page)
     :disambiguation (.isDisambiguationPage page)
     :category (.getCategories page)
     :link (.getLinks page)}))

(defn make-callback [f]
  (reify PageCallbackHandler
    (process [_ page]
      (f page))))

(defn make-parser [target handler]
  (doto (WikiXMLParserFactory/getSAXParser (.jurl target))
    (.setPageCallback (make-callback handler))))
