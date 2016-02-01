(ns stream2es.opts
  (:require [clojure.tools.cli :refer [cli]]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.es :as es]
            [stream2es.util.data :refer [re-replace-keys]]))

(def indexing-threads 2)

(def offset-field :__s2e_offset__)

(def common
  [["-d" "--max-docs" "Number of docs to index"
    :default -1
    :parse-fn #(Integer/parseInt %)]
   ["-q" "--queue-size" "Size of the internal bulk queue"
    :default 40
    :parse-fn #(Integer/parseInt %)]
   ["--stream-buffer" "Buffer up to this many pages"
    :default 50
    :parse-fn #(Integer/parseInt %)]
   ["--stream-timeout" "Wait seconds for data on the stream"
    :default -1
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--skip" "Skip this many docs before indexing"
    :default 0
    :parse-fn #(Integer/parseInt %)]
   ["-v" "--version" "Print version" :flag true :default false]
   ["-w" "--workers" "Number of indexing threads"
    :default indexing-threads
    :parse-fn #(Integer/parseInt %)]
   ["--clobber" "Use elasticsearch 'index' operation, clobbering existing documents, no-clobber uses 'create' which will skip/error existing documents" :flag true :default false]
   ["--tee-errors" "Create error-{id} files" :flag true :default true]
   ["--tee" "Save json request payloads as files in path"]
   ["--tee-bulk" "Save bulk request payloads as files in path"]
   ["--mappings" "Index mappings" :default nil]
   ["--settings" "Index settings" :default nil]
   ["--replace" "Delete index before streaming" :flag true :default false]
   ["--indexing" "Whether to actually send data to ES"
    :flag true :default true]
   ["--offset" (format
                (str "Add %s field TO EACH DOCUMENT with "
                     "the sequence offset of the stream")
                (name offset-field))
    :flag true :default false]
   ["--authinfo" "Stored stream credentials"
    :default (str
              (System/getProperty "user.home")
              (java.io.File/separator)
              ".authinfo.stream2es")]
   ["--target" "ES location" :default "http://localhost:9200"]
   ["-h" "--help" "Display help" :flag true :default false]
   ["--log" (format "Log level (%s)"
                    (->> taoensso.timbre/levels-ordered
                         (interpose " ")
                         (map name)
                         (apply str)))
    :default "info"]
   ["--http-insecure" "Don't verify peer cert" :flag true :default false]
   ["--http-keystore" "/path/to/keystore"]
   ["--http-keystore-pass" "Keystore password"]
   ["--http-trust-store" "/path/to/keystore"]
   ["--http-trust-store-pass" "Truststore password"]
   ])

(defn need-help? [tok]
  (when (some (partial = tok) ["help" "--help" "-help" "-h"])
    (throw+ {:type :help}))
  (when (some (partial = tok) ["version" "--version" "-version" "-v"])
    (throw+ {:type :version})))

(defn expand-http [[opts args x]]
  [(assoc opts
     :http (re-replace-keys opts #"^http-"))
   args
   x])

(defn attach-http-target [[opts args x]]
  [(assoc opts
     :target (es/make-target (:target opts) (:http opts)))
   args
   x])

(defn parse [args specs]
  (try
    (-> (apply cli args specs)
        expand-http
        attach-http-target)
    (catch Exception e
      (throw+ {:type ::badarg} (.getMessage e)))))
