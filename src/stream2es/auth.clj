(ns stream2es.auth
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [oauth.client :as oauth]
            [slingshot.slingshot :refer [try+ throw+]]
            [stream2es.log :as log]
            [stream2es.util.io :as uio])
  (:import (java.io FileNotFoundException)))

(def make-oauth-consumer oauth/make-consumer)

(defn get-token! [consumer]
  (let [request (oauth/request-token consumer)
        uri (oauth/user-approval-uri consumer (:oauth_token request))
        _ (do
            (println (apply str (take 80 (repeat "-"))))
            (println "Visit this URL...")
            (println)
            (println "  " uri)
            (println)
            (print "...and enter VERIFICATION CODE: ")
            (flush))
        code (-> System/in io/reader .readLine .trim)]
    (oauth/access-token consumer request code)))

(defn get-creds
  ([authinfo]
     (try
       (->> (slurp authinfo) edn/read-string)
       (catch FileNotFoundException _
         (throw+ {:type ::nocreds}
                 "credentials %s not found" authinfo))))
  ([authinfo & credtypes]
     (let [eligible (fn [entry]
                      (if (seq credtypes)
                        ((into #{} credtypes) (:type entry))
                        entry))]
       (->> (get-creds authinfo)
            (filter eligible)
            (sort-by :created)))))

(defn get-current-creds [authinfo type]
  (last (get-creds authinfo type)))

(defn store-creds [authinfo creds]
  (let [current (get-creds authinfo)]
    (->> creds
         (conj (get-creds authinfo))
         pp/pprint
         with-out-str
         (spit authinfo)))
  (uio/chmod-0600 authinfo))

