(ns stream2es.main
  (:gen-class)
  ;; Need to require these because of the multimethod in s.stream.
  (:require [cheshire.core :as json]
            [clojure.tools.cli :refer [cli]]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.bootstrap :refer [boot help]]
            [stream2es.es :as es]
            [stream2es.log :as log]
            [stream2es.opts :as opts]
            [stream2es.size :refer [size-of]]
            [stream2es.stream :as stream]
            [stream2es.util.io :as io]
            [stream2es.util.string :as s]
            [stream2es.util.time :as time]
            [stream2es.version :refer [version]])
  (:import (java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit)))

(def quit? true)

(defrecord BulkItem [meta source])

(defn quit
  ([code s]
     (quit code "%s" s))
  ([code fmt & s]
     (log/info (apply format fmt s))
     (when quit?
       (log/flush)
       (shutdown-agents)
       (System/exit code))))

(defn source2item [_index _type offset store-offset? source]
  (let [s2e-meta (:__s2e_meta__ source)]
    (BulkItem.
     {:index
      (merge
       {:_type (:_type s2e-meta _type)}
       (when (:_id s2e-meta)
         {:_id (str (:_id s2e-meta))})
       (when (:_routing s2e-meta)
         {:_routing (:_routing s2e-meta)})
       (when (:_parent s2e-meta)
         {:_parent (:_parent s2e-meta)}))
      :_bytes (-> source json/encode .getBytes count)}
     (merge (dissoc source :__s2e_meta__)
            (when store-offset?
              {opts/offset-field offset})))))

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
          (let [bulk-bytes (reduce + (map #(get-in % [:meta :_bytes]) bulk))
                url (format "%s/%s" (:target @state) "_bulk")
                errors (when (:indexing @state)
                         (es/error-capturing-bulk url bulk
                                                  make-indexable-bulk))]
            (dosync
             (alter state update-in [:total :indexed :docs] + (count bulk))
             (alter state update-in [:total :indexed :bytes] + bulk-bytes)
             (alter state update-in [:total :indexed :wire-bytes]
                    + idxbulkbytes)
             (alter state update-in [:total :errors] (fnil + 0) (or errors 0)))
            (index-status first-id (count bulk) idxbulkbytes state))
          (log/debug "adding indexed total"
                     (get-in @state [:total :indexed :docs])
                     "+" (count bulk))
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
               (quit 0 "done"))]
    (.start (Thread. done "lifecycle"))
    (.addShutdownHook
     (Runtime/getRuntime) (Thread. end "SIGTERM handler"))
    (dosync
     (alter state assoc :collector-notifier collector-notifier)
     (alter state assoc :indexer-notifier indexer-notifier)
     (alter state assoc :indexer indexer))
    state))

(defn ensure-index [{:keys [stream target mappings settings replace]
                     :as opts}]
  (let [idx-url (es/index-url target)]
    (when replace
      (es/delete idx-url))
    (when-not (es/exists? idx-url)
      (log/info "create index" idx-url)
      (let [mappings (merge (stream/mappings stream opts)
                            (json/decode mappings true))
            settings (merge (stream/settings stream opts)
                            (json/decode settings true))]
        (es/put idx-url (json/encode
                         {:settings settings
                          :mappings mappings}))))))

(defn main [world]
  (let [state (start! world)]
    (try
      (when (:indexing @state)
        (ensure-index @state)
        (log/info
         (format "stream %s%sto %s"
                 (:cmd @state)
                 (if (:source @state)
                   (format " from %s " (:source @state))
                   " ")
                 (:target @state))))
      (when (:tee-bulk @state)
        (log/info (format "saving bulks to %s" (:tee-bulk @state))))
      (when (:tee @state)
        (log/info (format "saving json to %s" (:tee @state))))
      (stream! state)
      (catch java.net.ConnectException e
        (throw+ {:type ::network} "%s connection refused" (:target @state))))))

(defn -main [& args]
  (try+
   (main (boot args))
   (catch [:type :authorized] _
     (quit 0 (:message &throw-context)))
   (catch [:type :help] {:keys [msg]}
     (quit 0 (or msg (help))))
   (catch [:type :version] _
     (quit 0 (format "stream2es %s" (version))))
   (catch [:type ::done] _
     (quit 0 (:message &throw-context)))
   (catch [:type :stream2es.auth/nocreds] _
     (quit 11 (format "Error: %s" (:message &throw-context))))
   (catch [:type :stream2es.bootstrap/badcmd] _
     (quit 12 (format "badcmd: %s\n\n%s" (:message &throw-context) (help))))
   (catch [:type :stream2es.opts/badarg] _
     (quit 13 (format "badarg: %s\n\n%s" (:message &throw-context) (help))))
   (catch [:type ::network] _
     (quit 14 (format "Network error: %s" (:message &throw-context))))
   (catch [:type :stream-death] {:keys [msg]}
     (quit 15 (format "stream terminated: %s" msg)))
   (catch Object _
     (let [t (:throwable &throw-context)]
       (.printStackTrace t)
       (quit 99 "unexpected exception: %s" (str t))))))
