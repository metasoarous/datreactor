(ns dat.reactor.utils
  (:require #?@(:clj [[clojure.core.match :as match :refer [match]]
                      [taoensso.timbre :as log]]
                :cljs [[cljs.core.match :refer-macros [match]]
                       [cljs.pprint]])))


(defn log [& args]
  #?(:cljs (apply js/console.log args)
     :clj (log/info (first args) (rest args))))


(defn tr
  ([message x]
   (log message (with-out-str (#?(:clj clojure.pprint/pprint :cljs cljs.pprint/pprint) x)))
   x)
  ([x]
   (tr "" x)))



