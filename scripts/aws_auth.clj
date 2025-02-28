(ns aws-auth
  (:require [babashka.fs :as fs]
            [babashka.process :refer [shell]]
            [lread.status-line :as status]
            [clojure.string :as str]))

(def aws-env-file ".env") ;; Path to the AWS environment file
(def max-age-minutes 240) ;; Set the allowed age threshold

(def running-in-ci?
  (boolean (System/getenv "CI")))

(defn assert-not-on-ci! []
  (when running-in-ci?
    (status/die 1 "AWS credentials should be set up in the GitHub workflow.")))

(defn assert-on-ci! []
  (when-not running-in-ci?
    (status/die 1 "Only apply from Github!")))

(defn get-file-age-minutes [file]
  (when (fs/exists? file)
    (let [mod-time (fs/file-time->millis (fs/last-modified-time file))
          current-time (System/currentTimeMillis)]
      (double (/ (- current-time mod-time) 60000)))))

(defn extract-role-name-from-file [file]
  (when (fs/exists? file)
    (some->> (slurp file)                                   ;; Read file
             str/split-lines                                ;; Split into lines
             (filter #(str/starts-with? % "AWS_ROLE_ARN=")) ;; Find role line
             first
             (str/split #"=")
             second)))

(defn fetch-aws-role []
  (try
    (let [result (shell {:out :string :err :string} "aws sts get-caller-identity --query Arn --output text")]
      (cond
        (re-find #"ExpiredToken" (:err result))
        (status/die 1 "AWS session token expired. Please run 'assume -e' or 'aws sso login' to refresh creds.")

        (= 0 (:exit result)) (str/trim (:out result))
        :else
        (status/die 1 (str "Error retrieving AWS Role ARN via AWS CLI: " (:err result)))))
    (catch Exception e
      (let [error-message (.getMessage e)]
        (if (re-find #"ExpiredToken" error-message)
          (status/die 1 "AWS session token expired. Run 'assume -e' or 'aws sso login' to refresh creds.")
          (status/die 1 (str "Unexpected error while retrieving AWS Role ARN: " error-message)))))
    ))

(defn get-aws-role []
  (let [aws-profile (System/getenv "AWS_PROFILE")
        aws-session-token (System/getenv "AWS_SESSION_TOKEN")]
    (cond
      (or aws-profile aws-session-token) (fetch-aws-role)
      :else
      (status/die 1 "Neither AWS_PROFILE nor AWS_SESSION_TOKEN is set! Please configure AWS credentials."))))

(defn check-aws-env []
  (assert-not-on-ci!) ;; Ensure we are not running in GitHub Actions

  (if-not (fs/exists? aws-env-file)
    (status/die 1 "❌ AWS .env file does not exist - use assume -e!")
    (let [file-age (get-file-age-minutes aws-env-file)
          role-name (get-aws-role)]
      (cond
        (> file-age max-age-minutes)
        (status/line :detail (format "⏳ AWS .env file is too old (%.1f minutes) - re-run assume!" file-age))

        :else
        (status/line :detail (format "✅ AWS role: %s (env file age: %.1f minutes)" role-name file-age))))))

(defn -main [& _args]
  (assert-not-on-ci!)
  (status/line :head "Checking AWS setup")
  (check-aws-env))

;; default action when executing file directly
(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
