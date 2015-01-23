(ns stream2es.test.opts
  (:require [clojure.test :refer :all])
  (:require [stream2es.opts :as opts]
            :reload))

(deftest t-http-keys
  (is (= {:insecure true}
         (let [[opts _ _ ] (opts/parse ["--http-insecure"] opts/common)]
           (:http opts)))))
