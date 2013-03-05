(ns stream2es.stream.wiki
  (:require [stream2es.stream :refer [new Stream Streamable CommandLine]])
  (:import (java.net URL MalformedURLException)
           (org.elasticsearch.river.wikipedia.support
            PageCallbackHandler WikiPage
            WikiXMLParserFactory)))

(declare make-parser make-callback)

(def bulk-bytes (* 3 1024 1024))

(def latest-wiki
  (str "http://download.wikimedia.org"
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
     ["-q" "--queue" "Size of the internal bulk queue"
      :default 40
      :parse-fn #(Integer/parseInt %)]
     ["-i" "--index" "ES index" :default "wiki"]
     ["-t" "--type" "ES type" :default "page"]
     ["-u" "--url" "Wiki dump locator" :default latest-wiki]
     ["--stream-buffer" "Buffer up to this many pages"
      :default 50
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (make-runner [this {:keys [url]} handler]
    (let [stream (make-parser url handler)]
      (WikiStreamRunner. #(.parse stream)))))

(extend-type WikiPage
  Streamable
  (make-source [page]
    {:_id (.getID page)
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

(defn url [s]
  (try
    (URL. s)
    (catch MalformedURLException _
      (URL. (str "file://" s)))))

(defn make-parser [loc handler]
  (doto (WikiXMLParserFactory/getSAXParser (url loc))
    (.setPageCallback (make-callback handler))))

