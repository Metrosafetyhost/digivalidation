(require '[clj-yaml.core :as yaml])

(def terraform-version "1.8.3")
(def terraform-state-bucket-prefix "metrosafety-cloud-env-terraform-state-")
(def metrosafety-write-role "arn:aws:iam::837329614132:role/github_oidc_role" )
(def dependency-token "${{ steps.dependency-app.outputs.token }}")
(def matrix-env "${{ matrix.environment.env-name }}")
(def envconfig-output "${{ fromJson(needs.env-config.outputs.environments) }}")
(def uv-lock "hashfiles('uv.lock' != '')" )


(def push-tags
  {:push {:tags  ["*"]}})

(def workflow-dispatch
  {:workflow_dispatch
   {:inputs
    {:version {:description "version aka v.X.X.X"
               :required false
               :default "latest"}
}}})

(def concurrency
  {:concurrency  {:group "terraform-state"}})

(def setup-babashka
  {:name "Setup Babashka"
   :uses "turtlequeue/setup-babashka@v1.7.0"
   :with {:babashka-version "1.12.196"}})

(def checkout-code
  {:name "Check out repository code"
   :uses "actions/checkout@v4"})

(def checkout-code-with-tags
  (merge checkout-code
         {:with {:fetch-depth 0
                 :fetch-tags true}}))

(defn checkout-code-with-token [token]
  (merge checkout-code
         {:with {:token token}}))

(def perms
  {:contents "read"
   :id-token "write"})


(def perms-pullreq-package
  (merge perms {:pull-requests "write"
                :packages "read"}))

(def perms-terraform
  (merge perms {:actions "read"
                :deployments "write"
                :pull-requests "write"}))

(def data-env
  {:SLACK_CHANNEL "C05RQ8UB8P4"
   :SlACK_TOKEN   "${{ secrets.SLACK_TROVECI_TOKEN_TEMPO }}"})

(def install-uv
  {:name "Install uv"
   :uses "astral-sh/setup-uv@v5"
   :with
   {:enable-cache true
    :cache-dependency-glob "uv.lock"}})

(def get-python-runtime
  {:name "Get Python version"
   :shell "bash"
   :run "echo PYTHON_VERSION=$(cat .python-version) >> $GITHUB_ENV"
   })

(def install-python
  {:name "Install Python"
   :run "uv python install $PYTHON_VERSION"
   })

(def uv-install-project
  {:name "Install Python project"
   :run "uv sync --all-extras --dev"}
  )

(def run-tests
  {:name "Run tests"
   :run "uv run pytest"})

(def bb-test
  {:name "bb-check-aws"
   :run "bb check-aws"})

(def github-context
  {:name "Get Github context"
   :shell "bash"
   :run "echo $GITHUB_CONTEXT"
    :env
    {:GITHUB_CONTEXT "${{ toJson(github) }}"}})

(def setup-terraform
  {:name "Setup terraform"
   :uses "hashicorp/setup-terraform@v3"
   :with {:terraform_version terraform-version}})

(def get-github-app-token
  {:name "Get Github App token"
   :uses "actions/create-github-app-token@v1"
   :id "dependency-app"
   :with {:app-id 718810
          :private-key "${{ secrets.TROVE_CI_DEPENDENCY_PRIVATE_KEY }}"
          :owner  "trovemoney"}})

(def tf-deploy-env
  {:TF_VAR_env "prod" })

(def terraform-init
  {:name "Terraform Init"
   :run (str
         "git config --global url.\"https://git:${{ steps.dependency-app.outputs.token }}@github.com/\".insteadOf \"https://github.com/\"\n"
         "terraform init \n"
         "--backend-config=bucket=bucket=metrosafety-cloud-env-terraform-state-${{ matrix.environment.account-identifier }}\n"
         "-backend-config=key=${{ github.event.repository.name }}/terraform.${{ matrix.environment.env-name }}.tfstate")
   :working-directory "./tf"})

(defn config-aws-creds [{:keys [creds-arn if-lock] :or {if-lock true}}]
  {:name "Configure AWS Credentials"
   :if if-lock
   :uses "aws-actions/configure-aws-credentials@v4"
   :with {:role-to-assume creds-arn
          :aws-region "ap-southeast-2"
          :disable-retry true}})

(def env-config
  {:runs-on "ubuntu-latest"
   :outputs {:environments "${{ steps.env.outputs.environments }}"
             :version "${{ env.VERSION }}"}
   :env {:VERSION "${{ !github.event.inputs.comment_id && github.event.inputs.version || github.ref_name }}"
         :ENV_NAME "${{ github.event_name == 'push' && 'plat-dev' || format('{0}', inputs.environment) }}"}
   :steps [
    {:name "Convert latest to last tag"
     :if "env.VERSION == 'latest'"
     :run "echo VERSION=$(gh api /repos/${{ github.repository }}/git/refs/tags | jq -r '[.[].ref | select(test(\"refs/tags/v[0-9]+.[0-9]+.[0-9]+$\"))] | last | sub(\"^refs/tags/\"; \"\")') >> $GITHUB_ENV"
     :env {:GITHUB_TOKEN "${{ secrets.GITHUB_TOKEN }}"}}

    {:uses "actions/checkout@v4"}

    {:name "Environment config"
     :id "env"
     :run "echo environments=$(cat .github/data/environments.yml | yq -o=json | jq '[.[] | select(.\"env-name\" | IN(${{ env.ENV_NAME }}))]') >> $GITHUB_OUTPUT"}

    {:name "Environment config not found"
     :run "if [[ \"${{ steps.env.outputs.environments }}\" == \"[]\" ]]; then echo 'environment config not defined for: ${{ env.ENV_NAME }}'; exit 1; fi"}
           ]
   })

(def python-tests
  {:name "ci-pytests"
   :on {:workflow_dispatch {}
        :pull_request {:branches ["main" "master"]}}
   :permissions perms-pullreq-package
   :jobs
   (merge {:env-config env-config}
          {:py-test
       {:name     "Python Tests"
        :runs-on  "ubuntu-latest"
        :steps
        [checkout-code
         install-uv
         get-python-runtime
         install-python
         uv-install-project
         run-tests
         ]}})
   })

(spit "../.github/workflows/unit-test.yml" (yaml/generate-string python-tests :dumper-options {:flow-style :block}))

(def bb-ci
  {:name "babashka-tests"
   :on {:push { :tags "*"}
        :workflow_dispatch {}}
   :permissions perms-pullreq-package
   :jobs
   [ ;;{:env-config env-config}
    {:bb-test
    {:name     "Babashka tests"
     :runs-on  "ubuntu-latest"
     :steps
     [setup-babashka
      github-context
      checkout-code
      install-uv
      get-python-runtime
      install-python
      uv-install-project
      bb-test
      ]}}]})

;;(spit "../.github/workflows/bb-test.yml" (yaml/generate-string bb-ci :dumper-options {:flow-style :block}))

(def tf-plan
  {:name "terraform plan"
   :on (merge push-tags workflow-dispatch)
   :concurrency concurrency
   :permissions perms-terraform
   ;; :env data-env
   :jobs
   {:tf-plan
    {:name     "Terraform Plan"
     :needs    "env-config"
     :runs-on  "ubuntu-latest"
     :environment "prod" ;; matrix-env
     ;; :strategy {:fail-safe false
     ;;            :matrix {:environment envconfig-output }}
     :env  tf-deploy-env
     :steps
     [setup-babashka
      setup-terraform
      get-github-app-token
      checkout-code
      install-uv
      get-python-runtime
      install-python
      (config-aws-creds {:creds-arn metrosafety-write-role})
      terraform-init
      {:name "Terraform Plan"
       :run "bb plan"
       :shell "bash"}
      ]}
    }
   })

(spit "../.github/workflows/terraform-plan.yml" (yaml/generate-string tf-plan :dumper-options {:flow-style :block}))


(def tf-apply
  {:name "terraform apply"
   :on (merge push-tags workflow-dispatch)
   :concurrency concurrency
   :permissions perms-terraform
   ;; :env data-env
   :jobs
   {:tf-apply
     {:name     "Terraform Apply"
      ;; :needs    "env-config"
      :runs-on  "ubuntu-latest"
      :environment "prod" ;;matrix-env
      ;; :strategy {:fail-safe false
      ;; :matrix {:environment envconfig-output }}
      :env  tf-deploy-env
      :steps
      [setup-babashka
       setup-terraform
       get-github-app-token
       checkout-code
       get-python-runtime
       install-uv
       install-python
       (config-aws-creds {:creds-arn metrosafety-write-role})
       terraform-init
       {:name "Terraform Apply"
        :run "bb apply"
        :shell "bash"}
       ]}
     }
   })

(spit "../.github/workflows/terraform-apply.yml" (yaml/generate-string tf-apply :dumper-options {:flow-style :block}))
