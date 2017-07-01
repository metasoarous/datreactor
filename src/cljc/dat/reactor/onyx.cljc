(ns dat.reactor.onyx
  #?(:cljs (:require-macros [cljs.core.async.macros :as async-macros :refer [go go-loop]]))
  (:require #?@(:clj [[clojure.core.async :as async :refer [go go-loop]]]
                :cljs [[cljs.core.async :as async]])
            [taoensso.timbre :as log #?@(:cljs [:include-macros true])]
            [dat.spec.protocols :as protocols]
            [onyx-local-rt.api :as onyx-api]
            [dat.reactor]
            [dat.sync.core :as dat.sync]
            [dat.sys.db]
            [com.stuartsierra.component :as component]))

(defn go-react! [{:keys [onyx-env event-chan react-chans kill-chan]}]
  (go-loop []
    (let [[event port] (async/alts! [kill-chan event-chan] :priority true)]
      (if (= port kill-chan)
        (log/info "go-react! received kill-chan signal")
        (do
          (try
            (let [env-after (-> onyx-env
                                (onyx-api/new-segment :dat.reactor/event event)
                                (onyx-api/drain)
                                (onyx-api/stop))]
              (doseq [[out-task out-chan] react-chans]
                (doseq [out-seg (get-in env-after [:tasks out-task :outputs])]
                  (async/>! out-chan out-seg))))
            (catch #?(:cljs :default :clj Exception) e
              (log/error e "Exception in reactor go loop")
              #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))))
          (recur))))))

;; ## Onyx Predicates
(def ^:export always (constantly true))

(defn ^:export not-nil?
  "flow-control - "
  [event old-seg seg all-new]
  (not (nil? seg)))

(defn ^:export transaction?
  [event old-seg seg all-new]
  (contains? seg :txs))

(defn ^:export localize?
  [event old-seg seg all-new]
  ;; TODO: decide which segments get sent to server
  ;; TODO: handle all peers not just server
  (= (:dat.reactor/event seg) :dat.sync/gdatoms))

(defn ^:export legacy?
  [event old-seg seg all-new]
  (= (:dat.reactor/event seg) :dat.reactor/legacy))

(defn ^:export source-from-tx-report?
[event old-seg seg all-new]
  (= (:dat.sync/event-source seg) :dat.sync/tx-report))

(defn ^:export source-from-remote?
[event old-seg seg all-new]
  (= (:dat.sync/event-source seg) :dat.sync/remote))



(defn +db-snap [conn]
  (map
    (fn [event]
      (assoc event
        :dat.sync/db-snap @conn))))

(defn handler-chan! [handler handler-fn & {:keys [chan]}]
  ;; ???: needs a kill-chan?
  ;; ???: could be implemented with async/pipeline maybe
  (let [chan (or chan (async/chan))]
    (go-loop []
      (let [seg (async/<! chan)]
        (handler-fn handler seg))
        (recur))
    chan))

(defn transact-segment! [transactor {:keys [txs]}]
  (protocols/transact! transactor txs))

(defn send-segment! [remote {:as seg :keys [:dat.remote/peer-id]}]
  (if peer-id
    (protocols/send-event! remote peer-id (dissoc seg :dat.remote/peer-id))
    (protocols/send-event! remote seg)))

(defn dispatch-segment! [dispatcher {:as seg :keys [:dat.reactor/dispatch-level]}]
  (if dispatch-level
    (protocols/dispatch! dispatcher seg dispatch-level)
    (protocols/dispatch! dispatcher seg)))

(defn legacy-segment! [{:as app :keys [conn]} {:as seg :keys [event]}]
  (log/info "process legacy event" event)
  (let [final-meta (atom nil)]
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

(defmulti event-msg-handler
  ; Dispatch on event-id
  (fn [app {:as event-msg :keys [id]}] id))

;; ## Event handlers

;; don't really need this... should delete
(defmethod event-msg-handler :chsk/ws-ping
  [_ _]
;;   (log/debug "Ping")
  )

;; Setting up our two main dat.sync hooks

;; General purpose transaction handler
(defmethod event-msg-handler :dat.sync.remote/tx
  [{:as app :keys [datomic]} {:as event-msg :keys [id ?data]}]
  (log/info "tx recieved from client: " id)
  (let [tx-report @(dat.sync/apply-remote-tx! (:conn datomic) ?data)]
    (println "Do something with:" tx-report)))

;; We handle the bootstrap message by simply sending back the bootstrap data
(defmethod event-msg-handler :dat.sync.client/bootstrap
  ;; What is send-fn here? Does that wrap the uid for us? (0.o)
  [{:as app :keys [datomic remote]} {:as event-msg :keys [id uid send-fn]}]
  (log/info "Sending bootstrap message")
  (protocols/send-event! remote uid [:dat.sync.client/bootstrap (protocols/bootstrap datomic)]))

;; Fallback handler; should send message saying I don't know what you mean
(defmethod event-msg-handler :default ; Fallback
  [app {:as event-msg :keys [event id ?data ring-req ?reply-fn send-fn]}]
  (log/warn "Unhandled event:" id))

(defn legacy-server-segment! [app seg]
  (event-msg-handler app seg))


(def onyx-batch-size 20) ;; FIXME: move to config

;; ## Onyx Reaction Job
(def default-job
  {:workflow [[:dat.reactor/event :dat.reactor/legacy]
              [:dat.reactor/event :dat.sync/localize] [:dat.sync/localize :dat.reactor/transact]
;;               [:dat.reactor/event :dat.reactor/dispatch]
              [:dat.reactor/event :dat.sync/handle-legacy-tx-report] [:dat.sync/handle-legacy-tx-report :dat.reactor/remote]
              [:dat.reactor/event :dat.sync/snap-transact] [:dat.sync/snap-transact :dat.sync/globalize] [:dat.sync/globalize :dat.reactor/remote]]
   :catalog [{:onyx/type :input
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.reactor/event}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.reactor/remote}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.reactor/transact}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.reactor/dispatch}
             {:onyx/type :output
              :onyx/name :dat.reactor/legacy
              :onyx/batch-size onyx-batch-size}
             {:onyx/type :function
              :onyx/name :dat.sync/localize
              :onyx/fn :dat.sync.core/gdatoms->local-txs
              :onyx/batch-size onyx-batch-size}
             {:onyx/type :function
              :onyx/name :dat.sync/snap-transact
              :onyx/fn :dat.sync.core/snap-transact
              :onyx/batch-size onyx-batch-size}
             {:onyx/type :function
              :onyx/name :dat.sync/globalize
              :onyx/fn :dat.sync.core/tx-report->gdatoms
              :onyx/batch-size onyx-batch-size}
             {:onyx/type :function
              :onyx/name :dat.sync/handle-legacy-tx-report
              :onyx/fn :dat.sync.core/handle-legacy-tx-report
              :onyx/batch-size onyx-batch-size}

             ]
   :flow-conditions [{:flow/from :dat.reactor/event
                      :flow/to [:dat.sync/snap-transact]
                      :flow/predicate ::transaction?}
                     {:flow/from :dat.reactor/event
                      :flow/to [:dat.sync/handle-legacy-tx-report]
                      :flow/predicate ::source-from-tx-report?}
                     {:flow/from :dat.reactor/event
                      :flow/to [:dat.sync/localize]
                      :flow/predicate ::localize?}
                     {:flow/from :dat.reactor/event
                      :flow/to [:dat.reactor/legacy]
                      :flow/predicate ::legacy?}
;;                      {:flow/from :dat.reactor/authorize
;;                       :flow/to [:dat.reactor/localize :dat.reactor/remote]
;;                       :flow/predicate ::auth}
                     ]})

(defrecord OnyxReactor [app dispatcher transactor remote event-chan kill-chan react-chans onyx-env server?]
  component/Lifecycle
  (start [reactor]
    (log/info "Starting OnyxReactor Component")
      (let [react-chans (or react-chans {:dat.reactor/transact (handler-chan! transactor transact-segment!)
                                         :dat.reactor/remote (handler-chan! remote send-segment!)
                                         :dat.reactor/dispatch (handler-chan! dispatcher dispatch-segment!)
                                         :dat.reactor/legacy (handler-chan! app (if server? legacy-server-segment! legacy-segment!))})
            event-chan (or event-chan (if server? (protocols/dispatcher-event-chan dispatcher) (async/chan)))
            onyx-env (or onyx-env (onyx-api/init default-job))
            ;; Start transaction process, and stash kill chan
            kill-chan (or kill-chan (async/chan))
            reactor (assoc reactor
                      :kill-chan kill-chan
                      :event-chan event-chan
                      :react-chans react-chans
                      :onyx-env onyx-env)]
        (when-not server?
          (async/pipeline
            1
            event-chan
            (comp (dat.sync/legacy-event><seg)
                  (+db-snap (:conn app)))
            (protocols/dispatcher-event-chan dispatcher)))
        (go-react! reactor)
        reactor))
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


