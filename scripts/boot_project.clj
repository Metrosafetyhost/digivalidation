(ns boot-project
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [selmer.parser :as selmer]
            [clj-yaml.core :as yaml]
            [lread.status-line :as status]
            [babashka.process :refer [shell]]))

;; Define paths
(def repo-root (fs/cwd))
(def pyproject-path (fs/file repo-root "pyproject.toml"))
(def python-version "3.13")
(def python-version-path (fs/file repo-root ".python-version"))
(def backend-hcl-template (fs/file repo-root ".github/data/backend.hcl"))

(def autoplan-env "prod")

(defn description-from-repo-name [repo-name]
  (->> (str/split repo-name #"-")
       (map str/capitalize)
       (str/join " ")))

(defn reinit-git []
  (let [git-dir (fs/file repo-root ".git")]
    (when (fs/exists? git-dir)
      (fs/delete-tree git-dir)
      (status/line :head "Deleted existing .git directory"))
    (shell "git init")
    (status/line :head "Initialised a new git repository")))

(defn bootstrap-repo []
  (let [repo-name (fs/file-name repo-root)
        pyproject-content (slurp pyproject-path)
        updated-pyproject-content (selmer/render pyproject-content {"SERVICENAME" repo-name "PYTHON_VERSION" python-version
                                                                    "DESCRIPTION" (description-from-repo-name repo-name)
                                                                    "EMAIL" "pablito@metrosaftey.co.nz"})
        backend-hcl-template-content (slurp backend-hcl-template)
        backend-hcl-rendered (selmer/render backend-hcl-template-content {"ENV" autoplan-env "SERVICENAME" repo-name})]
    (do (reinit-git)
        ;; Update files
        (status/line :head "Updating repo template files.")
        (spit pyproject-path updated-pyproject-content)
        (spit python-version-path python-version)
        (spit (str "tf/backend-" autoplan-env ".hcl") backend-hcl-rendered)
        (status/line :head "About to create uv.lock")
        (shell (str "uv sync  --python " python-version))
        (status/line :head "About to do initial commit")
        (shell "git add .")
        (shell (str "git commit -am \"Initial " repo-name " repo commit\""))
        (status/line :head "you should be ready to roll! bb for dev!")
        (shell "bb tasks"))))
