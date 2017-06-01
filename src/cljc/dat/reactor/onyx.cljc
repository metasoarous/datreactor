(ns dat.reactor.onyx
  #?(:cljs (:require-macros [cljs.core.async.macros :as async-macros :refer [go go-loop]]))
  (:require #?@(:clj [[clojure.core.async :as async :refer [go go-loop]]]
                :cljs [[cljs.core.async :as async]])
            ;#?(:clj [clojure.tools.logging :as log])
            [taoensso.timbre :as log #?@(:cljs [:include-macros true])]
            [dat.spec.protocols :as protocols]
            [onyx-local-rt.api :as onyx-api]
            [dat.reactor]
            ;[dat.reactor.utils :as utils]
            [dat.reactor.dispatcher :as dispatcher]
            [taoensso.timbre :as log #?@(:cljs [:include-macros true])]
            [datascript.core :as d]
            [com.stuartsierra.component :as component]))

;; (defmulti onyx-task-effect!
;;   (fn [_ {:keys [task-name]}]
;;     task-name))

;; (defmethod onyx-task-effect!
;;   :render
;;   [{:keys [mount react]} {:keys [outputs]}]
;;   (for [{:keys [path render]} outputs]
;;     ;; ???: aggregate representations from path into container for path
;;     (mount (react render))))

;; (defmethod onyx-task-effect!
;;   :re-onyx
;;   [{:keys [event-chan]} {:keys [outputs]}]
;;   (for [out outputs]
;;     (async/put! event-chan out)))

;; (defmethod onyx-task-effect!
;;   :replikativ
;;   [{:keys [tx-chan]} {:keys [outputs]}]
;;     (async/put! tx-chan (map :txs) outputs))

;; (defmethod onyx-task-effect!
;;   :transact
;;   [{:keys [conn]} {:keys [outputs]}]
;;   ;; TODO: move into replikativ middleware
;;   (d/transact! conn (into [] (map :txs) outputs)))

(defn go-transact! [{:as reactor :keys [conn outs]}]
  (go-loop []
    (d/transact! conn )
    (recur)))

(defn process-onyx-event! [{:as reactor :keys [onyx conn outs]} task-name event]
  (log/info "Processing onyx event" event)
    [[:db.fn/call
      (fn [db]
        (let [env-after (-> (:env onyx)
                            (onyx-api/new-segment :event (assoc event :db db))
                            (onyx-api/drain))]
          (sequence
            (comp
              (filter :outputs)
              (map (fn [{:keys [task-name outputs]}]
                     (async/put!
                       (get outs task-name)
                       outputs))))
            (:tasks env-after))))]])


(defn go-react!
  "Starts a go loop that processes events and effects using the handle-event! and
  execute-effect! fns. Effects are executed in sequence after the transaction commits.
  If a handler fails, the effects will not fire (will eventually support control over
  this behavior)."
  [{:as reactor :keys [conn]} app]
  (let [event-chan (protocols/dispatcher-event-chan (:dispatcher reactor))
        kill-chan (async/chan)]
    (go-loop []
      ;; Should probably use dispatcher api's version of event-chan here...
      (let [[event port] (async/alts! [kill-chan event-chan] :priority true)
            final-meta (atom nil)]
        ;(log/debug "Reactor recieved event with id:" (first event))
        (try
          (when-not (= port kill-chan)
            (when (map? event)
              (process-onyx-event! reactor :event event))
            (when (vector? event)
              (swap!
                conn
                (fn [current-db]
                  (try
                    (let [new-db (dat.reactor/handle-event! reactor current-db event)]
                      (reset! final-meta (meta new-db))
                      ;; Here we dissoc the effects, because we need to not let them stack up
                      (with-meta new-db (dissoc (meta new-db) :dat.reactor/effects)))
                    ;; We might just want to have our own error channel here, and set an option in the reactor
                    (catch #?(:clj Exception :cljs :default) e
                      (log/error e "Exception in reactor swap for event: " event)
                      #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))
                      ;(dispatch-error! reactor [:dat.reactor/error {:error e :event event}])
                      current-db))))
              (when-let [effects (seq (:dat.reactor/effects @final-meta))]
                (doseq [effect effects]
                  ;; Not sure if the db will pass through properly here so that effects execute on the db values
                  ;; immediately following their execution trigger
                  (dat.reactor/execute-effect! app (or (:db (meta effect)) @conn) effect)))))
          (catch #?(:cljs :default :clj Exception) e
            (log/error e "Exception in reactor go loop")
            #?(:clj (.printStackTrace e) :cljs (js/console.log (.-stack e)))))
        (if (= port kill-chan)
          (log/info "go-react! process recieved kill-chan signal")
          (recur))))
    kill-chan))


(def default-job
  {:workflow []
   :catalog []
    })

;; ## Component

;; Now for our actual component
(defrecord OnyxReactor [app dispatcher reactor conn kill-chan onyx outs]
  component/Lifecycle
  (start [reactor]
    (log/info "Starting OnyxReactor Component")
    (try
      (let [conn (or conn (:conn app) (d/create-conn))
            app (or app {:conn conn :reactor reactor :dispatcher dispatcher})
            outs (or outs {:transact (async/chan)
                           :replikativ (async/chan)
                           :re-onyx (async/chan)
                           :render (async/chan)})
            onyx (or onyx {:env (onyx-api/init default-job)})
            reactor (assoc reactor
                      :onyx onyx
                      :outs outs
                      :app app
                      :conn conn)
            ;; Start transaction process, and stash kill chan
            kill-chan (go-react! reactor app)
            reactor (assoc reactor
                           :kill-chan kill-chan)]
        (go-transact! reactor)
        reactor)
      (catch #?(:clj Exception :cljs :default) e
        (log/error "Error starting OnyxReactor:" e)
        #?(:clj (.printStackTrace e)
           :cljs (js/console.log (.-stack e)))
        reactor)))
  (stop [reactor]
    (when kill-chan (go (async/>! kill-chan :kill)))
    (assoc reactor
      :onyx nil
      :outs nil)))

(defn new-onyx-reactor
  "If :app is specified, it is reacted on. If not, it is computed as a map of {:dispatcher :reactor :conn}"
  ([options]
   (map->OnyxReactor options))
  ([]
   (new-onyx-reactor {})))


