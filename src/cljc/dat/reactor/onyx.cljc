(ns dat.reactor.onyx
  #?(:cljs (:require-macros [cljs.core.async.macros :as async-macros :refer [go go-loop]]))
  (:require #?@(:clj [[clojure.core.async :as async :refer [go go-loop]]]
                :cljs [[cljs.core.async :as async]])
            [taoensso.timbre :as log #?@(:cljs [:include-macros true])]
            [dat.spec.protocols :as protocols]
            [onyx-local-rt.api :as onyx-api]
            [datascript.core :as ds]
            [dat.reactor]
            [dat.sync.core :as dat.sync]
            [dat.sys.db]
            [com.stuartsierra.component :as component]))

(defn conj-job
  ;; ???: should flow conditions be ordered completely by an order function
  ([] {:catalog #{}
       :workflow #{}
       :flow-conditions []})
  ([job]
   ;; TODO: validate job
   job)
  ([{:as job :keys [catalog workflow flow-conditions]} more-job]
   ;; ???: use deep merge? or single layer deep merge?
   ;; TODO: add other onyx job keys like windowing and triggers
   (assoc job
     :catalog (into catalog (:catalog more-job))
     :workflow (into workflow (:workflow more-job))
     :flow-conditions (into flow-conditions (:flow-conditions more-job)))))

(defn remove-outputs [env task-name]
  (assoc-in env [:tasks task-name :outputs] []))

(defn loopback-drain [env]
  (loop [env env]
    (let [drained-env (onyx-api/drain env)
          loop-segs (get-in drained-env [:tasks :dat.reactor/loopback :outputs])]
      ;; ???: check for infinite loop
      (if-not loop-segs
        drained-env
        (recur (-> drained-env
                   (remove-outputs :dat.reactor/loopback)
                   (onyx-api/new-segment :dat.reactor/loop-in loop-segs)))))))

(defn go-react! [{:keys [onyx-atom event-chan kill-chan]}]
  (go-loop []
    (let [[event port] (async/alts! [kill-chan event-chan] :priority true)]
      (if (= port kill-chan)
        (log/info "go-react! received kill-chan signal")
        (do
          (try
            (let [{:keys [env job]} @onyx-atom
                  env-after (-> env
                                (onyx-api/new-segment (:dat.reactor/input event) event)
                                (loopback-drain)
                                (onyx-api/stop))]
              (log/debug "reacting to event of type:"
                         (:dat.reactor/event event) (:id event))
              (doseq [{:as task :keys [dat.reactor/chan]}
                      (filter
                        #(= (:onyx/type %) :output)
                        (:catalog job))]
                (doseq [out-seg (get-in env-after [:tasks (:onyx/name task) :outputs])]
                  ;; ???: should callbacks be allowed?
                  (async/>! chan out-seg))))
            (catch #?(:cljs :default :clj Exception) e
              (log/error e "Exception in reactor go loop")
              #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))))
          (recur))))))

(defn go-react-deprecated! [{:keys [onyx-atom event-chan react-chans kill-chan]}]
  (go-loop []
    (let [[event port] (async/alts! [kill-chan event-chan] :priority true)]
      (if (= port kill-chan)
        (log/info "go-react! received kill-chan signal")
        (do
          (try
            (let [env-after (-> (:env @onyx-atom)
                                (onyx-api/new-segment :dat.reactor/event event)
                                (loopback-drain)
                                (onyx-api/stop))]
              (doseq [[out-task out-chan] react-chans]
                (doseq [out-seg (get-in env-after [:tasks out-task :outputs])]
                  ;; ???: should callbacks be allowed?
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
  [{:as app :keys [transactor remote]} {:as event-msg :keys [uid send-fn]}]
  (log/info "Sending bootstrap message" uid event-msg)
  (protocols/send-event! remote uid [:dat.sync.client/bootstrap (protocols/bootstrap transactor)]))

;; Fallback handler; should send message saying I don't know what you mean
(defmethod event-msg-handler :default ; Fallback
  [app {:as event-msg :keys [event id ?data ring-req ?reply-fn send-fn]}]
  (log/warn "Unhandled event:" id))

(defn legacy-server-segment! [app seg]
  (event-msg-handler app seg))

(defn go-new-inputs! [{:as onyx :keys [job running-chans]} event-chan]
  (log/debug "go-new-inputs")
  (let [new-input-tasks
        (sequence
          (comp
            (filter #(= (:onyx/type %) :input))
            (filter :dat.reactor/chan)
            (remove #(contains? running-chans (:dat.reactor/chan %))))
          (:catalog job))]
    (doseq [{:as task :keys [dat.reactor/chan]} new-input-tasks]
      (log/debug "listening for inputs task" (:onyx/name task))
      (go-loop []
        (let [event (async/<! chan)]
          (async/>! event-chan (assoc (dat.sync/legacy-event->seg event) :dat.reactor/input (:onyx/name task)))
          (recur))))
  (assoc onyx
    :running-chans
    (into
      (or running-chans #{})
      (map :dat.reactor/chan)
      new-input-tasks))))

(defn expand-job!
  "Expands reactor's onyx job to include the given job fragment. The job-key is used to allow overwriting a job fragment previously registered. This function has side effects, but is semi-idempotent (you can safely call multiple times with the same fragment)."
  [{:as reactor :keys [event-chan onyx-atom]} job-key fragment]
  ;; ???: mechanism to give fragments/flow-control order?
  ;; ???: transducer middleware for conj-job?
  ;; TODO: ensure valid onyx spec
;;   (log/debug "expanding job")
  (swap!
    onyx-atom
    (fn [{:as onyx :keys [job-fragments]}]
      (let [fragments (assoc (or job-fragments {}) job-key fragment)
            job (transduce (map second) conj-job fragments)]
;;         (log/debug "full job" job)
        (-> onyx
            (merge
              {:job-fragments fragments
               :job job
               :env (onyx-api/init job)})
            (go-new-inputs! event-chan)
            )))))


(def onyx-batch-size 20) ;; FIXME: move to config


(defn wire-connection
  "Deprecated"
  ([wire nspace]
   (wire-connection wire (keyword nspace "recv") (keyword nspace "send")))
  ([wire recv-name send-name]
   {:catalog
    [{:onyx/type :output
      :onyx/name send-name
      :dat.reactor/chan (protocols/send-chan wire)
      :onyx/batch-size onyx-batch-size}
     {:onyx/type :input
      :onyx/name recv-name
      :dat.reactor/chan (protocols/recv-chan wire)
      :onyx/batch-size onyx-batch-size}]}))

;; ## Onyx Reaction Job
(def base-job
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
             {:onyx/type :input
              :onyx/name :dat.reactor/loop-in
              :onyx/batch-size onyx-batch-size}
             {:onyx/type :output
              :onyx/name :dat.reactor/loopback
              :onyx/batch-size onyx-batch-size}
             {:onyx/type :output
              :onyx/batch-size onyx-batch-size
              :onyx/name :dat.reactor/transact}
;;              {:onyx/type :output
;;               :onyx/batch-size onyx-batch-size
;;               :onyx/name :dat.reactor/dispatch}
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

(defrecord OnyxReactor [onyx-atom event-chan kill-chan
                        app dispatcher transactor remote server?]
  component/Lifecycle
  (start [reactor]
    (log/info "Starting OnyxReactor Component")
      (let [running? kill-chan ;; ???: move kill-chan creation back into the go-react! loop
            reactor (assoc reactor
                      :onyx-atom (or onyx-atom (atom {}))
                      :kill-chan (or kill-chan (async/chan))
                      :event-chan (or event-chan
                                      #?(:cljs (async/chan 1 (+db-snap (:conn app))) ;; FIXME
                                         :clj  (async/chan))))]
        (expand-job!
          reactor
          ::base-job
          {:catalog
           [{:onyx/type :input
             :onyx/name :dat.reactor/loop-in
             :onyx/batch-size onyx-batch-size}
            {:onyx/type :output
             :onyx/name :dat.reactor/loopback
             :onyx/batch-size onyx-batch-size}
            {:onyx/type :output
             :onyx/name :dat.reactor/legacy
             :dat.reactor/chan (handler-chan!
                                 app ;; !!!: bad
                                (if server? legacy-server-segment! legacy-segment!)) ;; ???: idempotent
             :onyx/batch-size onyx-batch-size}

            {:onyx/type :input
             :onyx/batch-size onyx-batch-size
             :dat.reactor/chan (protocols/recv-chan dispatcher)
             :onyx/name :dat.view.dom/event}
;;             {:onyx/type :output
;;              :onyx/batch-size onyx-batch-size
;;              :dat.reactor/chan (protocols/send-chan dispatcher)
;;              :onyx/name :dat.view.dom/render}
            {:onyx/type :input
             :onyx/batch-size onyx-batch-size
             :dat.reactor/chan (protocols/recv-chan remote)
             :onyx/name :dat.remote/recv}
            {:onyx/type :output
             :onyx/batch-size onyx-batch-size
             :dat.reactor/chan (protocols/send-chan remote)
             :onyx/name :dat.remote/send}
            {:onyx/type :input
             :onyx/batch-size onyx-batch-size
             :dat.reactor/chan (protocols/recv-chan transactor)
             :onyx/name :dat.db/tx-report}
            {:onyx/type :output
             :onyx/batch-size onyx-batch-size
             :dat.reactor/chan (handler-chan! transactor transact-segment!) ;;(protocols/send-chan transactor)
             :onyx/name :dat.db/transact}
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
           :workflow
           [
            [:dat.view.dom/event :dat.reactor/legacy]
            [:dat.remote/recv :dat.reactor/legacy]
            [:dat.remote/recv :dat.sync/localize] [:dat.sync/localize :dat.db/transact]
            [:dat.db/tx-report :dat.sync/handle-legacy-tx-report] [:dat.sync/handle-legacy-tx-report :dat.remote/send]
            [:dat.view.dom/event :dat.sync/snap-transact] [:dat.sync/snap-transact :dat.sync/globalize] [:dat.sync/globalize :dat.remote/send]
            ]
           :flow-conditions
           [{:flow/from :dat.view.dom/event
             :flow/to [:dat.sync/snap-transact]
             :flow/predicate ::transaction?}
            {:flow/from :dat.remote/recv
             :flow/to [:dat.sync/localize]
             :flow/predicate ::localize?}
            {:flow/from :dat.view.dom/event
             :flow/to [:dat.reactor/legacy]
             :flow/predicate ::legacy?}
            {:flow/from :dat.remote/recv
             :flow/to [:dat.reactor/legacy]
             :flow/predicate ::legacy?}]
           }
          )
        (when (not running?)
          (go-react! reactor))
        reactor))
  (stop [reactor]
    (when kill-chan (async/put! kill-chan :kill))
    ;; ???: close kill-chan, event-chan, or running-chans inside the onyx-atom?
    (assoc reactor
      :event-chan nil
      :onyx-atom nil
      :kill-chan nil)))

(defrecord OnyxReactorDeprecated [app dispatcher transactor remote onyx-atom event-chan kill-chan react-chans server?]
  component/Lifecycle
  (start [reactor]
    (log/info "Starting OnyxReactor Component")
      (let [react-chans (or react-chans {:dat.reactor/transact (handler-chan! transactor transact-segment!)
                                         :dat.reactor/remote (protocols/send-chan remote)
;;                                          :dat.reactor/dispatch (protocols/send-chan dispatcher)
                                         :dat.reactor/legacy (handler-chan! app (if server? legacy-server-segment! legacy-segment!))})
            event-chan (or event-chan
                           (if server?
                             (protocols/recv-chan dispatcher)
                             (let [chan> (async/chan)]
                               (async/pipeline
                                 1
                                 chan>
                                 (comp (dat.sync/legacy-event><seg)
                                       (+db-snap (:conn app)))
                                 (protocols/recv-chan dispatcher))
                               chan>)))
            ;; Start transaction process, and stash kill chan
            kill-chan (or kill-chan (async/chan))
            reactor (assoc reactor
                      :onyx-atom (or onyx-atom
                                    (atom {:env (onyx-api/init base-job)
                                           :job-fragments {:dat.reactor/base-job base-job}}))
                      :kill-chan kill-chan
                      :event-chan event-chan
                      :react-chans react-chans)]
        (go-react! reactor)
        reactor))
  (stop [reactor]
    (when kill-chan (async/put! kill-chan :kill))
    (assoc reactor
      :event-chan nil
      :onyx-atom nil
      :react-chans nil
      :kill-chan nil)))

(defn new-onyx-reactor
  "If :app is specified, it is reacted on. If not, it is computed as a map of {:dispatcher :reactor :conn}"
  ([options]
   (map->OnyxReactor options))
  ([]
   (new-onyx-reactor {})))


