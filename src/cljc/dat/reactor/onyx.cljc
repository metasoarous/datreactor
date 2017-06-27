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
        (do
          (try
            (let [env-after (-> onyx-env
                                (onyx-api/new-segment :dat.sync/event event)
                                (onyx-api/drain)
                                (onyx-api/stop))]
              (doseq [[out-task out-chan] react-chans]
                (doseq [out-seg (get-in env-after [:tasks out-task :outputs])]
                  (async/>! out-chan out-seg))))
            (catch #?(:cljs :default :clj Exception) e
              (log/error e "Exception in reactor go loop")
              #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))))
          (recur))))))

(def onyx-batch-size 20) ;; FIXME: move to config

;; (defmulti handle-legacy-event (fn [db [event-id _]] event-id))

;; (defmethod handle-legacy-event :default [db event]
;;   (log/warn "Unhandled legacy event " event))

;; (defn ^:export legacy [{:as seg :keys [event]}]
;;   {:txs
;;    [[:db.fn/call
;;      (fn [db]
;;        (handle-legacy-event db event))]]})


;; ## Onyx Predicates
(def ^:export always (constantly true))

(defn ^:export not-nil?
  "flow-control - "
  [event old-seg seg all-new]
  (not (nil? seg)))

(defn ^:export transaction?
  [event old-seg seg all-new]
  (contains? seg :txs))

(defn ^:export server?
  [event old-seg seg all-new]
  ;; TODO: decide which segments get sent to server
  ;; TODO: handle all peers not just server
  false)

(defn ^:export legacy?
  [event old-seg seg all-new]
  (= (:dat.sync/event seg) :dat.sync.event/legacy))


;; ## Onyx Reaction Job
(def default-job
  {:workflow [[:dat.sync/event :dat.sync/legacy]
              [:dat.sync/event :dat.sync/transactor]
              [:dat.sync/event :dat.sync/server]]
   :catalog [{:onyx/type :input
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.sync/event}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.sync/server}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.sync/transactor}
             {:onyx/type :output
              :onyx/name :dat.sync/legacy
              :onyx/batch-size onyx-batch-size}]
   :flow-conditions [{:flow/from :dat.sync/event
                      :flow/to [:dat.sync/transactor]
                      :flow/predicate ::transaction?}
                     {:flow/from :dat.sync/event
                      :flow/to [:dat.sync/server]
                      :flow/predicate ::server?}
                     {:flow/from :dat.sync/event
                      :flow/to [:dat.sync/legacy]
                      :flow/predicate ::legacy?}]})

(defn legacy-event><seg []
  (map (fn [event]
         (if (vector? event)
           {:dat.sync/event :dat.sync.event/legacy
            :event event}
           event))))

(defn chan-middleware! [middleware-transducer & {:as options :keys [buff in-chan]}]
  ;; ???: needs a kill-chan option?
  (let [out-chan (async/chan (or buff 1) middleware-transducer)]
    (when in-chan
      (go-loop []
        (let [event (async/<! in-chan)]
          (log/info "process event" event)
          (async/>! out-chan event)
        (recur))))
    out-chan))

(defn handler-chan! [handler handler-fn & {:keys [chan]}]
  ;; ???: needs a kill-chan?
  (let [chan (or chan (async/chan))]
    (go-loop []
      (let [seg (async/<! chan)]
        (handler-fn handler seg))
        (recur))
    chan))

(defn transact-segment! [transactor {:keys [txs]}]
  (protocols/transact! transactor txs))

(defn send-segment! [remote seg]
  (protocols/send-event! remote seg))

(defn legacy-segment! [{:as app :keys [conn]} {:as seg :keys [event]}]
  (log/info "process legacy event" event)
  (let [;;reactor {:app app :conn conn}
        final-meta (atom nil)]
    ;; TODO: fix fake reactor refs. conn already in app. very bad form.
    (swap!
      conn
      (fn [current-db]
        (try
          (let [new-db (dat.reactor/handle-event! app current-db event)]
            (reset! final-meta (meta new-db))
            ;; Here we dissoc the effects, because we need to not let them stack up
            (with-meta new-db (dissoc (meta new-db) :dat.reactor/effects)))
          ;; We might just want to have our own error channel here, and set an option in the reactor
          (catch #?(:clj Exception :cljs :default) e
            (log/error e "Exception in reactor swap for legacy event: " event)
            #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))
            ;(dispatch-error! reactor [::error {:error e :event event}])
            current-db))))
    (when-let [effects (seq (:dat.reactor/effects  @final-meta))]
      (doseq [effect effects]
        ;; Not sure if the db will pass through properly here so that effects execute on the db values
        ;; immediately following their execution trigger
        (dat.reactor/execute-effect! app (or (:db (meta effect)) @conn) effect)))))

(defrecord OnyxReactor [app dispatcher transactor remote event-chan kill-chan react-chans onyx-env]
  component/Lifecycle
  (start [reactor]
    (log/info "Starting OnyxReactor Component")
    (try
      (let [react-chans (or react-chans {:dat.sync/transactor (handler-chan! transactor transact-segment!)
                                         :dat.sync/server (handler-chan! remote send-segment!)
                                         :dat.sync/legacy (handler-chan! app legacy-segment!)})
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


