(ns wiki2es.size)

(def type-sizes
  {Boolean 1/8
   Character 2
   Byte 1
   Short 2
   Integer 4
   Float 4
   Long 8
   Double 8})

(defn unknown-type [x]
  (throw
   (Exception.
    (with-out-str
      (print "don't know size of" (type x) "")
      (prn x)))))

(defn size-of-number [n]
  (or (type-sizes (type n))
      (unknown-type n)))

(defn vector?* [x]
  (or (vector? x)
      (= java.util.Vector (type x))))

(defn list?* [x]
  (or (list? x)
      (= java.util.ArrayList (type x))))

(defn linear? [x]
  (some true? ((juxt sequential? vector?* list?*) x)))

(defn boolean? [x]
  (= Boolean (type x)))

(defn size-of [x]
  (Math/round
   (float
    (cond
      (nil? x) 4
      (map? x) (reduce + (map size-of (vals x)))
      (linear? x) (reduce + (map size-of x))
      (string? x) (* (type-sizes Character) (count x))
      (number? x) (size-of-number x)
      (boolean? x) (type-sizes Boolean)
      :else (unknown-type x)))))
