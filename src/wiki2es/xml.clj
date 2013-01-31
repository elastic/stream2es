(ns wiki2es.xml
  (:import (java.net URL MalformedURLException)
           (org.elasticsearch.river.wikipedia.support
            PageCallbackHandler
            WikiXMLParserFactory)))

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
