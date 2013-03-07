(ns stream2es.test.main
  (:require [clojure.test :refer :all])
  (:require [stream2es.main] :reload))

(deftest help
  (with-redefs [stream2es.main/quit (fn [& args] (first args))
                stream2es.main/main (fn [& args])]
    (testing "no args"
      (is (nil? (stream2es.main/-main))))
    (testing "good cmd"
      (is (nil? (stream2es.main/-main "stdin"))))
    (testing "single --help"
      (is (.startsWith (stream2es.main/-main "--help") "Copyright")))
    (testing "badcmd"
      (is (.contains (stream2es.main/-main "foo") "foo is not a")))))
