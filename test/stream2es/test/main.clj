(ns stream2es.test.main
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [stream2es.es]
            [stream2es.stream.twitter :as twitter])
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
      (let [one {:settings (twitter/make-settings)
                 :mappings (twitter/make-mapping "thing")}
            two (json/decode
                 (stream2es.main/ensure-index
                  {:replace false
                   :stream stream
                   :index "test"
                   :type "thing"
                   :settings settings})
                 true)]
        (testing "use defaults"
          (is (not
               (nil?
                (get-in one [:mappings :thing :properties :created_at]))))
          (is (= (get-in one [:mappings :thing
                              :properties :created_at :format])
                 (get-in two [:mappings :thing
                              :properties :created_at :format])))))

      (testing "merge defaults"
        (reset! ops [])
        (let [res (json/decode
                   (stream2es.main/ensure-index
                    {:replace true
                     :stream stream
                     :index "test"
                     :type "thing"})
                   true)]
          (is (= "text" (get-in res [:settings :query.default_field])))
          (is (= @ops [:delete :exists? :post])))))))
