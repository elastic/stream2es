(ns stream2es.stream.wiki
  (:require [clojure.java.io :as io]
            [clojure.data.xml :as xml]
            [stream2es.stream :refer [new Stream Streamable
                                      StreamStorage CommandLine]])
  (:import (org.apache.commons.compress.compressors.bzip2
            BZip2CompressorInputStream)))

(declare make-parser make-callback)

(def bulk-bytes (* 3 1024 1024))

(def latest-wiki
  (str "http://download.wikimedia.org"
       "/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"))

(defrecord WikiStream [])

(defrecord WikiStreamRunner [runner])

(defrecord WikiPage [root])

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
      (WikiStreamRunner. #(.parse stream))))
  StreamStorage
  (settings [_]
    {:number_of_shards 2
     :number_of_replicas 0
     :query.default_field :text})
  (mappings [_ type]
    {(keyword type)
     {:_all {:enabled false}
      :properties {}}}))

(extend-type WikiPage
  Streamable
  (make-source [page]
    #_{:_id (.getID page)
       :title (-> page .getTitle str .trim)
       :text (-> (.getText page) .trim)
       :redirect (.isRedirect page)
       :special (.isSpecialPage page)
       :stub (.isStub page)
       :disambiguation (.isDisambiguationPage page)
       :category (.getCategories page)
       :link (.getLinks page)}))

(defn make-parser [loc handler]
  (->>
   (-> loc
       io/input-stream
       BZip2CompressorInputStream.
       xml/parse
       :content)
   (filter #(#{:page} (:tag %)))
   (map ->WikiPage)
   (map handler)))
