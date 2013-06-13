(ns stream2es.test.main
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [stream2es.es]
            [stream2es.stream.twitter])
  (:require [stream2es.main] :reload))

(deftest help
  (let [res (atom nil)]
    (with-redefs [stream2es.main/quit (fn [& args]
                                        (reset! res (first args)))
                  stream2es.main/main (fn [_])]
      (testing "no args"
        (stream2es.main/-main)
        (is (nil? @res)))
      (testing "good cmd"
        (stream2es.main/-main "stdin")
        (is (nil? @res)))
      (testing "single --help"
        (stream2es.main/-main "--help")
        (is (.startsWith @res "Copyright")))
      (testing "badcmd"
        (stream2es.main/-main "foo")
        (is (.startsWith @res "Error: foo is not a"))))))

(deftest index-settings
  (let [ops (atom [])
        stream (stream2es.stream.twitter.TwitterStream.)
        settings (json/encode stream2es.main/index-settings)]
    (with-redefs [stream2es.es/post (fn [_ _ payload]
                                      (swap! ops conj :post)
                                      payload)
                  stream2es.es/delete (fn [& _]
                                        (swap! ops conj :delete))
                  stream2es.es/exists? (fn [& _]
                                         (swap! ops conj :exists?)
                                         false)]
      (testing "use defaults"
        (is (= {:settings (json/decode settings true)
                :mappings {:thing
                           {:_all {:enabled false}
                            :properties
                            {:location {:type "geo_point"}}}}}
               (json/decode
                (stream2es.main/ensure-index
                 {:replace false
                  :stream stream
                  :index "test"
                  :type "thing"
                  :settings settings})
                true))))

      (testing "merge defaults"
        (reset! ops [])
        (let [mappings (json/encode
                        {:thing
                         {:properties
                          {:location {:type "long"}}}})]
          (is (= {:settings stream2es.main/index-settings
                  :mappings {:thing
                             {:properties
                              {:location {:type "long"}}}}}
                 (json/decode
                  (stream2es.main/ensure-index
                   {:replace true
                    :stream stream
                    :index "test"
                    :type "thing"
                    :settings settings
                    :mappings mappings})
                  true))))))))
