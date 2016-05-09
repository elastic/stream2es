(ns stream2es.bootstrap
  (:require [slingshot.slingshot :refer [throw+]])
  (:require [stream2es.auth :as auth]
            [stream2es.help :as help]
            [stream2es.opts :as opts]
            [stream2es.log :as log])
  (:require [stream2es.stream :as stream]
            [stream2es.stream.es]
            [stream2es.stream.generator]
            [stream2es.stream.stdin]
            [stream2es.stream.twitter :as twitter]
            [stream2es.stream.wiki]))

(defn help-preamble []
  (with-out-str
    (println "Copyright 2013 Elasticsearch")
    (println)
    (println "Usage: stream2es [CMD] [OPTS]")
    (println)
    (println "Available commands: wiki, twitter, stdin, es")
    (println)
    (println "Common opts:")
    (print (help/help opts/common))))

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

(defn maybe-get-stream [args]
  (let [cmd (if (seq args)
              (let [tok (first args)]
                (opts/need-help? tok)
                (symbol tok))
              'stdin)]
    (try
      [cmd (stream/new cmd)]
      (catch IllegalArgumentException _
        (throw+ {:type ::badcmd} "%s is not a valid command" cmd)))))

(defn boot [args]
  (let [[cmd stream] (maybe-get-stream args)
        [optmap args _] (opts/parse args (concat opts/common
                                                 (stream/specs stream)))]
    (log/init! optmap)
    (when (:help optmap)
      (throw+ {:type :help :msg (help stream)}))
    (when (and (= cmd 'twitter) (:authorize optmap))
      (auth/store-creds (:authinfo optmap) (twitter/make-creds optmap))
      (throw+
       {:type :authorized}
       "*** Success! Credentials saved to %s" (:authinfo optmap)))
    (merge
     (assoc optmap :stream stream :cmd cmd)
     (stream/bootstrap stream optmap))))
