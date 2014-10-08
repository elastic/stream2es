(ns stream2es.log
  (:refer-clojure :exclude [flush])
  (:require [taoensso.timbre :as t]
            [clojure.string :as str])
  (:import (java.util.concurrent Executors)))

(t/merge-config!
 {:timestamp-pattern "yyyy-MM-dd'T'HH:mm:ss.SSSZ"
  :timestamp-locale java.util.Locale/ROOT
  :fmt-output-fn
  (fn [{:keys [level throwable message timestamp hostname ns]}
       & [{:keys [nofonts?] :as appender-fmt-output-opts}]]
    (format "%s %-5s %s%s" timestamp
            (-> level name str/upper-case)
            (or message "")
            (or (t/stacktrace throwable "\n" (when nofonts? {})) "")))})

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
