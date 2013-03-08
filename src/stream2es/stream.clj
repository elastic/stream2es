(ns stream2es.stream)

(defn dispatch-first [& args]
  (first args))

(defmulti new dispatch-first)

(defprotocol CommandLine
  (specs [s]
    "Command-line options specific to this stream impl."))

(defprotocol Streamable
  (make-source [obj]
    "Make map of stream Java source object."))

(defprotocol StreamStorage
  (settings [stream type]
    "Index mapping/settings"))

(defprotocol Stream
  (make-runner [stream opts publisher]
    "Make stream runner, wrapping up the handler that had to be
    created from the state passed in earlier."))
