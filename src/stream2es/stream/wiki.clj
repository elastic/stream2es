(ns stream2es.stream.wiki
  (:import (java.net URL MalformedURLException)
           (org.elasticsearch.river.wikipedia.support
            PageCallbackHandler
            WikiXMLParserFactory)))

(def bulk-bytes
  (* 3 1024 1024))

(def latest-wiki
  (str "http://download.wikimedia.org"
       "/enwiki/latest/enwiki-latest-pages-articles.xml.bz2"))

(def opts
  [["-b" "--bulk-bytes" "Bulk size in bytes"
    :default bulk-bytes
    :parse-fn #(Integer/parseInt %)]
   ["-q" "--queue" "Size of the internal bulk queue"
    :default 40
    :parse-fn #(Integer/parseInt %)]
   ["-i" "--index" "ES index" :default "wiki"]
   ["-t" "--type" "ES type" :default "page"]
   ["-u" "--url" "Wiki dump locator" :default latest-wiki]
   ["--stream-buffer" "Buffer up to this many tweets"
    :default 1000
    :parse-fn #(Integer/parseInt %)]])

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

(defn make-source [page]
  {:_id (.getID page)
   :title (-> page .getTitle str .trim)
   :text (-> (.getText page) .trim)
   :redirect (.isRedirect page)
   :special (.isSpecialPage page)
   :stub (.isStub page)
   :disambiguation (.isDisambiguationPage page)
   :category (.getCategories page)
   :link (.getLinks page)})

(defn make-stream [url handler]
  (let [stream (make-parser url handler)]
    {:run #(.parse (:stream %))
     :stream stream}))
