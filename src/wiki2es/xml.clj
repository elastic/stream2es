(ns wiki2es.xml
  (:import (edu.jhu.nlp.wikipedia PageCallbackHandler
                                  WikiXMLParserFactory)))

(defn make-callback [f]
  (reify PageCallbackHandler
    (process [_ page]
      (f page))))

(defn make-parser [loc handler]
  (doto (WikiXMLParserFactory/getSAXParser loc)
    (.setPageCallback (make-callback handler))))

