(ns build
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
            [babashka.process :refer [shell]]
            [selmer.parser :as selmer]
            [lread.status-line :as status]))

(def project-file "pyproject.toml")

(defn extract-pyproject-name [file-path]
  (when (fs/exists? file-path)
    (let [ data (slurp file-path)
          ;; Extract the [project] section including content until the next header
          project-section (some->> (re-find #"(?s)\[project\](.*?)(?=\n\[|\z)" data) second)
          ;; Extract `name = "value"` within [project]
          name-match (when project-section
                       (re-find #"(?m)^\s*name\s*=\s*\"([^\"]+)\"" project-section))
          _ (status/line :detail (str "✔️ using Service Name: " (second name-match) " from: " file-path ))]
      (when name-match
        (second name-match)))))

(defn build-layer [layers-dir layer-zip python-runtime]
  ;; Ensure layers-dir exists before creating files inside it
  (fs/create-dirs layers-dir)

  (let [layer-target (str layers-dir "/python/lib/python" python-runtime "/site-packages/")
        layer-requirements-file (str layers-dir "/layer-requirements.txt")]
    (if (or (not (fs/exists? layer-zip))
            (> (fs/file-time->millis (fs/last-modified-time project-file))
               (fs/file-time->millis (fs/last-modified-time layer-zip)))) ;; when pyproject updated then rebuild
      (do
        (status/line :detail "👷‍♀️ Building Lambda Layer...")
        (shell {:out :string :err :string} "uv" "export" "--frozen" "--no-dev" "--no-editable"
               "--no-emit-project" "-o" layer-requirements-file)
        (shell {:out :string :err :string} "uv" "pip" "install" "--no-installer-metadata" "--no-compile-bytecode"
               "--python-platform" "aarch64-manylinux2014" "--python" python-runtime
               "--target" layer-target "-r" layer-requirements-file)
        (fs/zip layer-zip layers-dir {:root layers-dir})
        (status/line :detail (str "🏗️ Created Layer zip:" layer-zip)))
      (status/line :detail "🏁 Lambda Layer Zip doesnt need updating"))))

(defn build-lambdas [lambdas-dir runtime-dir python-version]
  ;; Ensure runtime-dir exists before creating files inside it
  (fs/create-dirs runtime-dir)

  (let [lambda-files (->> (fs/list-dir lambdas-dir "*.py")
                          (remove #(= (fs/file-name %) "__init__.py"))
                          (map fs/file-name))
        lambdas-map (map (fn [lambda]
                           {:file-name lambda
                            :lambda-name (fs/strip-ext lambda {:ext "py"})})
                         lambda-files)
        tf-vars-file "tf/lambda.auto.tfvars"
        template "lambda_file_names = [{% for lambda in lambdas %} \"{{ lambda.file-name }}\" {% if not forloop.last %}, {% endif %}{% endfor %}]
lambda_names = [{% for lambda in lambdas %} \"{{ lambda.lambda-name }}\" {% if not forloop.last %}, {% endif %}{% endfor %}]
runtime = \"{{ runtime }}\"\n"
        tf-content (selmer/render template {:lambdas lambdas-map :runtime (str "python" python-version)})]

    (doseq [file lambda-files]
      (let [file-prefix (fs/strip-ext file {:ext "py"})
            zip-file (fs/path runtime-dir (str file-prefix ".zip"))
            zip-file-exists? (fs/exists? zip-file) ;; Check if the zip file exists
            zip-file-time (if zip-file-exists?
                            (fs/file-time->millis (fs/last-modified-time zip-file))
                            0) ;; Default to 0 if it doesn't exist
            src-file-time (fs/file-time->millis (fs/last-modified-time (fs/path lambdas-dir file)))]
        (when (> src-file-time zip-file-time)
          (status/line :detail (str "🏗️ Zipping" file "->" zip-file))
          (fs/zip zip-file [(fs/path lambdas-dir file)] {:root lambdas-dir})
          (fs/move zip-file runtime-dir {:replace-existing true}))))

    (spit tf-vars-file tf-content)
    (println "Generated Terraform vars file:" tf-vars-file)))
