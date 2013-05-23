(ns stream2es.stream.stdin
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [stream2es.stream :refer [new Stream
                                      Streamable CommandLine
                                      StreamStorage]]))

(defrecord StdinStream [])

(defrecord StdinStreamRunner [runner])

(defmethod new 'stdin [_]
  (StdinStream.))

(extend-type StdinStream
  CommandLine
  (specs [_]
    [["-b" "--bulk-bytes" "Bulk size in bytes"
      :default (* 1024 100)
      :parse-fn #(Integer/parseInt %)]
     ["-q" "--queue" "Size of the internal bulk queue"
      :default 40
      :parse-fn #(Integer/parseInt %)]
     ["-i" "--index" "ES index" :default "foo"]
     ["-t" "--type" "ES type" :default "t"]
     ["--stream-buffer" "Buffer up to this many docs"
      :default 100
      :parse-fn #(Integer/parseInt %)]])
  Stream
  (make-runner [_ opts handler]
    (StdinStreamRunner.
     (fn []
       (loop [in (io/reader *in*)]
         (let [line (.readLine *in*)]
           (if-not line
             (handler :eof)
             (do
               (handler line)
               (recur in))))))))
  StreamStorage
  (settings [_]
    {:number_of_shards 2
     :number_of_replicas 0})
  (mappings [_ type]
    {(keyword type)
     {:_all {:enabled false}
      :properties {}}}))

(extend-type String
  Streamable
  (make-source [doc]
    (json/decode doc true)))
