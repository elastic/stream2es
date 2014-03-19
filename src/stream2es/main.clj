(ns stream2es.main
  (:gen-class)
  ;; Need to require these because of the multimethod in s.stream.
  (:require [stream2es.stream.wiki]
            [stream2es.stream.stdin]
            [stream2es.stream.twitter :as twitter]
            [stream2es.stream.queue]
            [stream2es.stream.es])
  (:require [cheshire.core :as json]
            [clojure.tools.cli :refer [cli]]
            [stream2es.auth :as auth]
            [stream2es.log :as log]
            [stream2es.es :as es]
            [stream2es.size :refer [size-of]]
            [stream2es.version :refer [version]]
            [stream2es.stream :as stream]
            [stream2es.help :as help]
            [stream2es.util.io :as io]
            [stream2es.util.string :as s]
            [stream2es.util.time :as time]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (clojure.lang ExceptionInfo)
           (java.io FileNotFoundException)
           (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit)))

(def quit? true)

(def indexing-threads 2)

(def index-settings
  {:index.number_of_shards 2
   :index.number_of_replicas 0
   :index.refresh_interval :5s})

(def offset-field
  :_s2e_offset)

(def opts
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
   ["-h" "--help" "Display help" :flag true :default false]])

(defrecord BulkItem [meta source])

(defn quit
  ([]
     (quit ""))
  ([s]
     (quit "%s" s))
  ([fmt & s]
     (when (pos? (count (first s)))
       (println (apply format fmt s)))
     (when quit?
       (shutdown-agents)
       (System/exit 0))))

(defn source2item [_index _type offset store-offset? source]
  (BulkItem.
   {:index
    (merge
     {:_index _index
      :_type (:_type source _type)}
     (when (:_id source)
       {:_id (str (:_id source))})
     (when (:_routing source)
       {:_routing (:_routing source)}))
    :_bytes (-> source json/encode .getBytes count)}
   (merge (dissoc source :_id :_type :_routing)
          (when store-offset?
            {offset-field offset}))))

(defn flush-bulk [state]
  (let [itemct (count (:items @state))
        items (:items @state)]
    (when (pos? itemct)
      #_(log/info
         (format ">--> %d items; %d bytes; first-id %s"
                 itemct (:bytes @state)
                 (-> items first :meta :index :_id)))
      ((:indexer @state) items)
      (dosync
       (alter state assoc :bytes 0)
       (alter state assoc :items [])))))

(defn maybe-index [state]
  (let [{:keys [bytes bulk-bytes]} @state]
    (when (> bytes bulk-bytes)
      (flush-bulk state))))

(defn continue? [state]
  (let [curr (get-in @state [:total :streamed :docs])
        {:keys [skip max-docs]} @state]
    (if (pos? max-docs)
      (< curr (+ skip max-docs))
      true)))

(defn skip? [state]
  (>= (:skip @state) (get-in @state [:total :streamed :docs])))

(defn flush-indexer [state]
  (log/info "flushing index queue")
  (dotimes [_ (:workers @state)]
    ((:indexer @state) :stop)))

(defn want-shutdown [state]
  (log/debug "want shutdown")
  (flush-bulk state)
  (flush-indexer state))

(defn spit-mkdirs [path name data]
  (when path
    (let [sub (s/hash-dir 2)
          path (io/file path sub)
          f (io/file path (str name ".gz"))]
      (log/debug "save" (str f) (count (.getBytes data)) "bytes")
      (.mkdirs (io/file path))
      (io/spit-gz f data))))

(defn make-indexable-bulk [items]
  (->> (for [item items]
         (str (json/encode (:meta item))
              "\n"
              (json/encode (:source item))
              "\n"))
       (apply str)))

(defn make-json-string [items]
  (let [replace-id (fn [item]
                     (merge (:source item)
                            {:_id (-> item :meta :index :_id)}
                            (when (-> item :meta :index :_type)
                              {:_type (-> item :meta :index :_type)})))]
    (->> items
         (map replace-id)
         (map json/encode)
         (interpose "\n")
         (apply str))))

(defn index-status [id bulk-count bulk-bytes state]
  (let [upmillis (- (System/currentTimeMillis) (:started-at @state))
        upsecs (float (/ upmillis 1e3))
        index-doc-rate (/ (get-in @state [:total :indexed :docs]) upsecs)
        index-kbyte-rate (/
                          (/ (get-in @state [:total :indexed :wire-bytes]) 1024)
                          upsecs)
        #_stream-doc-rate #_(/ (get-in @state [:total :streamed :docs]) upsecs)
        #_stream-kbyte-rate #_(/
                               (/ (get-in @state [:total :streamed :bytes])
                                  1024)
                               upsecs)]
    (log/info
     (format "%s %.1fd/s %.1fK/s %d %d %d %d%s"
             (time/minsecs upsecs)
             index-doc-rate index-kbyte-rate
             (get-in @state [:total :indexed :docs])
             bulk-count bulk-bytes
             (get-in @state [:total :errors])
             (if id (format " %s" id) "")))))

(defn index-bulk [q state]
  (let [bulk (.take q)]
    (when-not (= :stop bulk)
      (when (and (sequential? bulk) (pos? (count bulk)))
        (let [first-id (-> bulk first :meta :index :_id)
              idxbulk (make-indexable-bulk bulk)
              idxbulkbytes (count (.getBytes idxbulk))]
          (when (:indexing @state)
            (let [bulk-bytes (reduce + (map #(get-in % [:meta :_bytes]) bulk))
                  url (format "%s/%s" (:target @state) "_bulk")
                  errors (es/error-capturing-bulk url bulk make-indexable-bulk)]
              (dosync
               (alter state update-in [:total :indexed :docs] + (count bulk))
               (alter state update-in [:total :indexed :bytes] + bulk-bytes)
               (alter state update-in [:total :indexed :wire-bytes]
                      + idxbulkbytes)
               (alter state update-in [:total :errors] (fnil + 0) errors))
              (index-status first-id (count bulk) idxbulkbytes state))
            (log/debug "adding indexed total"
                       (get-in @state [:total :indexed :docs])
                       "+" (count bulk)))
          (when (:tee-bulk @state)
            (spit-mkdirs
             (:tee-bulk @state) (str first-id ".bulk") idxbulk))
          (when (:tee @state)
            (let [data (make-json-string bulk)]
              (spit-mkdirs (:tee @state) (str first-id ".json") data)))))
      (recur q state))))

(defn start-indexer-pool [state]
  (let [q (LinkedBlockingQueue. (:queue-size @state))
        latch (CountDownLatch. (:workers @state))
        disp (fn []
               (index-bulk q state)
               (log/debug "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done indexing")
                    ((:indexer-notifier @state)))]
    ;; start index pool
    (dotimes [n (:workers @state)]
      (.start (Thread. disp (str "indexer " (inc n)))))
    ;; notify when done
    (.start (Thread. lifecycle "index service"))
    ;; This becomes :indexer above!
    (fn [bulk]
      (.put q bulk))))

(defn start-doc-stream [state process]
  (let [q (LinkedBlockingQueue. (:stream-buffer @state))
        latch (CountDownLatch. 1)
        stop (fn []
               (want-shutdown state)
               (.countDown latch))
        disp (fn []
               (let [obj (if (pos? (:stream-timeout @state))
                           (.poll q (:stream-timeout @state)
                                  TimeUnit/SECONDS)
                           (.take q))]
                 (if-not (and obj
                              (not (= :eof obj))
                              (continue? state))
                   (stop)
                   (do
                     (dosync
                      (alter state update-in [:total :streamed :docs] inc))
                     (when-not (skip? state)
                       (process obj)
                       (maybe-index state))
                     (recur)))))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done collecting")
                    ((:collector-notifier @state)))]
    (.start (Thread. disp "stream dispatcher"))
    (.start (Thread. lifecycle "stream service"))
    ;; publisher
    (fn [stream-object]
      (.put q stream-object))))

(defn make-object-processor [state]
  (fn [stream-object]
    (let [source (stream/make-source stream-object @state)]
      (when source
        (dosync
         (let [{:keys [index type]} (es/components (:target @state))
               item (source2item index type
                                 (get-in @state [:total :streamed :docs])
                                 (:offset @state)
                                 source)]
           (alter state update-in
                  [:bytes] + (-> item :meta :_bytes))
           (alter state update-in
                  [:total :streamed :bytes]
                  + (-> source str .getBytes count))
           (alter state update-in
                  [:items] conj item)))))))

(defn stream! [state]
  (let [process (make-object-processor state)
        publish (start-doc-stream state process)
        stream-runner (stream/make-runner (:stream @state) @state publish)]
    ((-> stream-runner :runner))))

(defn start! [opts]
  (let [state (ref
               (merge opts
                      {:started-at (System/currentTimeMillis)
                       :bytes 0
                       :items []
                       :total {:indexed {:docs 0
                                         :bytes 0
                                         :wire-bytes 0}
                               :streamed {:docs 0
                                          :bytes 0}}}))
        collector-latch (CountDownLatch. 1)
        indexer-latch (CountDownLatch. 1)
        collector-notifier #(.countDown collector-latch)
        indexer-notifier #(.countDown indexer-latch)
        indexer (start-indexer-pool state)
        printed-done? (atom false)
        end (fn []
              (when-not @printed-done?
                (log/info
                 (format
                  "streamed %d indexed %d bytes xfer %d errors %d"
                  (-> @state :total :streamed :docs)
                  (-> @state :total :indexed :docs)
                  (-> @state :total :indexed :wire-bytes)
                  (-> @state :total :errors)))
                (reset! printed-done? true)))
        done (fn []
               (log/debug "waiting for collectors")
               (.await collector-latch)
               (log/debug "waiting for indexers")
               (.await indexer-latch)
               (end)
               (quit))]
    (.start (Thread. done "lifecycle"))
    (.addShutdownHook
     (Runtime/getRuntime) (Thread. end "SIGTERM handler"))
    (dosync
     (alter state assoc :collector-notifier collector-notifier)
     (alter state assoc :indexer-notifier indexer-notifier)
     (alter state assoc :indexer indexer))
    state))

(defn parse-opts [args specs]
  (try
    (apply cli args specs)
    (catch Exception e
      (throw+ {:type ::badarg} (.getMessage e)))))

(defn help-preamble []
  (with-out-str
    (println "Copyright 2013 Elasticsearch")
    (println)
    (println "Usage: stream2es [CMD] [OPTS]")
    (println)
    (println "Available commands: wiki, twitter, stdin, queue, es")
    (println)
    (println "Common opts:")
    (print (help/help opts))))

(defn help-stream [& streams]
  (with-out-str
    (doseq [stream streams :let [inst (if (satisfies? stream/CommandLine stream)
                                        stream
                                        (.newInstance stream))]]
      (println)
      (println (format "%s opts:"
                       (second (re-find #"\.([^.]+)@0$" (str inst)))))
      (print (help/help (stream/specs inst))))))

(defn help [& streams]
  (with-out-str
    (print (help-preamble))
    (print
     (apply help-stream
            (if (seq streams)
              streams
              (extenders stream/CommandLine))))))

(defn get-stream [args]
  (let [cmd (if (seq args)
              (let [tok (first args)]
                (when (.startsWith tok "-")
                  (throw+ {:type ::badarg} ""))
                (symbol tok))
              'stdin)]
    (try
      [cmd (stream/new cmd)]
      (catch IllegalArgumentException _
        (throw+ {:type ::badcmd}
                "%s is not a valid command" cmd)))))

(defn ensure-index [{:keys [stream target mappings settings replace]
                     :as opts}]
  (let [idx-url (es/index-url target)]
    (when replace
      (es/delete idx-url))
    (when-not (es/exists? idx-url)
      (log/info "create index" idx-url)
      (let [mappings (merge (stream/mappings stream opts)
                            (json/decode mappings true))
            settings (merge index-settings
                            (stream/settings stream)
                            (json/decode settings true))]
        (es/put idx-url (json/encode
                         {:settings settings
                          :mappings mappings}))))))

(defn main [world]
  (let [state (start! world)]
    (try
      (when (:indexing @state)
        (ensure-index @state))
      (log/info
       (format "stream %s%sto %s"
               (:cmd @state)
               (if (:source @state)
                 (format " from %s " (:source @state))
                 " ")
               (:target @state)))
      (when (:tee-bulk @state)
        (log/info (format "saving bulks to %s" (:tee-bulk @state))))
      (when (:tee @state)
        (log/info (format "saving json to %s" (:tee @state))))
      (stream! state)
      (catch java.net.ConnectException e
        (quit "%s connection refused" (:target @state)))
      (catch Exception e
        (.printStackTrace e)
        (quit "stream error: %s" (str e))))))

(defn -main [& args]
  (try+
   (let [[cmd stream] (get-stream args)
         main-plus-cmd-specs (concat opts (stream/specs stream))
         [optmap args _] (parse-opts args main-plus-cmd-specs)]
     (when (:help optmap)
       (quit (help stream)))
     (when (and (= cmd 'twitter) (:authorize optmap))
       (auth/store-creds (:authinfo optmap) (twitter/make-creds optmap))
       (quit "*** Success! Credentials saved to %s" (:authinfo optmap)))
     (if (:version optmap)
       (quit (version))
       (main (merge
              (assoc optmap :stream stream :cmd cmd)
              (stream/bootstrap stream optmap)))))
   (catch [:type :stream2es.auth/nocreds] _
     (quit (format "Error: %s" (:message &throw-context))))
   (catch [:type ::badcmd] _
     (quit (format "Error: %s\n\n%s" (:message &throw-context) (help))))
   (catch [:type ::badarg] _
     (let [msg (format "Error: %s\n\n%s" (:message &throw-context) (help))]
       (quit msg)))
   (catch Object _
     (let [t (:throwable &throw-context)]
       (.printStackTrace t)
       (quit "unexpected exception: %s"
             (str t))))))
