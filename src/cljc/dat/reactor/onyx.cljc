(ns dat.reactor.onyx
  #?(:cljs (:require-macros [cljs.core.async.macros :as async-macros :refer [go go-loop]]))
  (:require #?@(:clj [[clojure.core.async :as async :refer [go go-loop]]]
                :cljs [[cljs.core.async :as async]])
            ;#?(:clj [clojure.tools.logging :as log])
            [taoensso.timbre :as log #?@(:cljs [:include-macros true])]
            [dat.spec.protocols :as protocols]
            [onyx-local-rt.api :as onyx-api]
            [dat.reactor]
            [dat.sys.db]
            ;[dat.reactor.utils :as utils]
            [dat.reactor.dispatcher :as dispatcher]
            [taoensso.timbre :as log #?@(:cljs [:include-macros true])]
            [datascript.core :as d]
            [com.stuartsierra.component :as component]))

(defn go-react! [{:keys [onyx-env event-chan react-chans kill-chan]}]
  (go-loop []
    (let [[event port] (async/alts! [kill-chan event-chan] :priority true)]
      (if (= port kill-chan)
        (log/info "go-react! received kill-chan signal")
        (try
          (let [env-after (-> onyx-env
                            (onyx-api/new-segment :dat.sync/event event)
                            (onyx-api/drain))]
          (for [[out-task out-chan] react-chans]
            (doseq [out-seg (get-in env-after [:tasks out-task :outputs])]
              (async/>! out-chan out-seg))))
          (catch #?(:cljs :default :clj Exception) e
            (log/error e "Exception in reactor go loop")
            #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))))
          (recur)))))

(def onyx-batch-size 20) ;; FIXME: move to config

(defmulti handle-legacy-event (fn [db [event-id _]] event-id))

(defmethod handle-legacy-event :default [db event]
  (log/warn "Unhandled legacy event " event))

(defn ^:export legacy [{:as seg :keys [event-vector]}]
  {:txs
   [[:db.fn/call
     (fn [db]
       (handle-legacy-event db event-vector))]]})

(def default-job
  {:workflow [[:dat.sync/event :dat.sync.event/legacy]
              [:dat.sync.event/legacy :dat.sync/transactor]
              [:dat.sync.event/legacy :dat.sync/server]]
   :catalog [{:onyx/type :input
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.sync/event}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.sync/server}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.sync/transactor}
             {:onyx/type :function
              :onyx/name :dat.sync.event/legacy
              :onyx/batch-size onyx-batch-size
              :onyx/fn ::legacy}]
   :flow-conditions []})

(defn legacy-event><seg []
  (map (fn [event]
         (if (vector? event)
           {:dat.sync/event :dat.sync.event/legacy
            :event-vector event}
           event))))

(defn chan-middleware! [middleware-transducer & {:as options :keys [buff in-chan]}]
  ;; ???: needs a kill-chan option?
  (let [out-chan (async/chan (or buff 1) middleware-transducer)]
    (when in-chan
      (go-loop []
        (async/>! out-chan (async/<! in-chan))
        (recur)))
    out-chan))

(defn handler-chan! [handler handler-fn & {:keys [chan]}]
  ;; ???: needs a kill-chan?
  (let [chan (or chan (async/chan))]
    (go-loop []
             (handler-fn remote (!< chan))
             (recur))
    chan))

(defrecord OnyxReactor [dispatcher transactor remote event-chan kill-chan react-chans onyx-env]
  component/Lifecycle
  (start [reactor]
    (log/info "Starting OnyxReactor Component")
    (try
      (let [react-chans (or react-chans {:dat.sync/transactor (handler-chan! transactor dat.sys.db/transact!)
                                         :dat.sync/server (handler-chan! remote protocols/send-event!)})
            event-chan (or event-chan (chan-middleware!
                                        (legacy-event><seg)
                                        :in-chan (protocols/dispatcher-event-chan dispatcher)))
            onyx-env (or onyx-env (onyx-api/init default-job))
            ;; Start transaction process, and stash kill chan
            kill-chan (or kill-chan (async/chan))
            reactor (assoc reactor
                        :kill-chan kill-chan
                        :event-chan event-chan
                        :react-chans react-chans
                        :onyx-env onyx-env)]
        (go-react! reactor)
        reactor)
      ;; ***TODO: add an onyx task for the remote or stitch it to the world
      (catch #?(:clj Exception :cljs :default) e
        (log/error "Error starting OnyxReactor:" e)
        #?(:clj (.printStackTrace e)
           :cljs (js/console.log (.-stack e)))
        reactor)))
  (stop [reactor]
    (when kill-chan (async/put! kill-chan :kill))
    (assoc reactor
      :onyx-env nil
      :event-chan nil
      :react-chans nil
      :kill-chan nil)))

(defn new-onyx-reactor
  "If :app is specified, it is reacted on. If not, it is computed as a map of {:dispatcher :reactor :conn}"
  ([options]
   (map->OnyxReactor options))
  ([]
   (new-onyx-reactor {})))


