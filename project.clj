(defproject datreactor "0.0.1-alpha1-SNAPSHOT"
  :description "Event and effect dispatch and handling (transactions/coordination) mechanisms for Cljc apps with DataScript databases"
  :url "http://github.com/metasoarous/datreactor"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v11.html"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.9.0-alpha6"]
                 [org.clojure/clojurescript "1.9.293"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/core.async "0.2.395"]
                 [org.clojure/tools.logging "0.3.1"]
                 ;; Datsys things
                 [datspec "0.0.1-alpha1-SNAPSHOT"]
                 [com.stuartsierra/component "0.3.2"]
                 ;; Other stuff
                 [datascript "0.15.5"]
                 [io.rkn/conformity "0.4.0"] ;; should this be here?
                 [com.taoensso/timbre "4.8.0"]
                 [prismatic/plumbing "0.5.3"]] ;; aren't using currently
  ;;
  ;; ## Snipped from DataScript's
  ;; ============================
  ;;
  ;; The following was taken from DataScript's project.clj; may need to clean up a bit
  ;;
  ;; Leaving this out for now
  ;:global-vars {*warn-on-reflection* true}
  :cljsbuild {:builds [{:id "release"
                        :source-paths ["src" "bench/src"]
                        :assert false
                        :compiler {:output-to     "release-js/datreactor.bare.js"
                                   :optimizations :advanced
                                   :pretty-print  false
                                   :elide-asserts true
                                   :output-wrapper false
                                   :parallel-build true}}]}
                        ;:notify-command ["release-js/wrap_bare.sh"]
  :profiles {:dev {:source-paths ["bench/src" "test" "dev" "src"]
                   :plugins [[lein-cljsbuild "1.1.2"]
                             [lein-typed "0.3.5"]]
                   :cljsbuild {:builds [{:id "advanced"
                                         :source-paths ["src" "bench/src" "test"]
                                         :compiler {:output-to     "target/datreactor.js"
                                                    :optimizations :advanced
                                                    :source-map    "target/datreactor.js.map"
                                                    :pretty-print  true
                                                    :recompile-dependents false
                                                    :parallel-build true}}
                                        {:id "none"
                                         :source-paths ["src" "bench/src" "test" "dev"]
                                         :compiler {:main          datreactor.test
                                                    :output-to     "target/datreactor.js"
                                                    :output-dir    "target/none"
                                                    :optimizations :none
                                                    :source-map    true
                                                    :recompile-dependents false
                                                    :parallel-build true}}]}}}
  :clean-targets ^{:protect false} ["target"
                                    "release-js/datreactor.bare.js"
                                    "release-js/datreactor.js"]
  ;;
  ;; ## Back to from extraction...
  ;; =============================
  ;;
  ;; Once we're ready
  ;:core.typed {:check []
               ;:check-cljs []}
  ;;
  ;; Not sure if we need these either
  :resource-paths ["resources" "resources-index/prod"]
  :target-path "target/%s"
  :aliases {"package"
            ["with-profile" "prod" "do"
             "clean" ["cljsbuild" "once"]]})


