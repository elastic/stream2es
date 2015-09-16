(ns stream2es.util.typed
  (:require [clojure.core.typed :as typed])
  (:require [slingshot.slingshot :as slingshot]))

;; http://stackoverflow.com/q/30947473/3227
(defmacro throw+ [object]
  `(let [o# ~object]
     (typed/tc-ignore
      (slingshot/throw+ o#))
     (throw (Exception. "something went wrong with slingshot/throw+"))))

(defmacro unnullable [place object]
  `(if ~object
     ~object
     (slingshot/throw+ {:type :bad-null-bad
                        :where ~place})))
