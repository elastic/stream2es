(ns stream2es.log
  (:refer-clojure :exclude [flush])
  (:require [taoensso.timbre :as t]
            [clojure.string :as str])
  (:import (java.util.concurrent Executors)))

(t/merge-config!
 {:timestamp-opts
  {:pattern     "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
   :locale      java.util.Locale/ROOT
   :timezone    :utc}
  :output-fn
  (fn out
    ([data] (out nil data))
    ([{:keys [no-stacktrace? stacktrace-fonts] :as opts} data]
     (let [{:keys [level ?err_ vargs_ msg_ ?ns-str hostname_
                   timestamp_ ?line]} data]
       (str
        (format "%s %-5s %s"
                @timestamp_
                (str/upper-case (name level))
                (force msg_))
        (when-not no-stacktrace?
          (when-let [err (force ?err_)]
            (str "\n" (t/stacktrace err opts))))))))})

(defn init! [opts]
  (t/set-level! (-> opts :log str/lower-case keyword)))

(defmacro trace [& msg]
  `(t/trace ~@msg))

(defmacro debug [& msg]
  `(t/debug ~@msg))

(defmacro info [& msg]
  `(t/info ~@msg))

(defmacro warn [& msg]
  `(t/warn ~@msg))

(defmacro error [& msg]
  `(t/error ~@msg))

(defn flush []
  (dotimes [_ 3]
    (clojure.core/flush)
    (Thread/sleep (rand-int 100))))
