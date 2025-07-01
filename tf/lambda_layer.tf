resource "aws_iam_role" "lambda_role" {
  name               = "bedrock_lambda_execution_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}


resource "aws_iam_policy" "bedrock_invoke_policy" {
  name        = "BedrockInvokePolicy"
  description = "Allows AWS Lambda to call AWS Bedrock models"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "bedrock:InvokeModel"
        Resource = "arn:aws:bedrock:eu-west-2::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0" #anthropic.claude-3-haiku-20240307-v1:0"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_bedrock_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.bedrock_invoke_policy.arn
}

resource "aws_iam_policy" "lambda_s3_access" {
  name        = "LambdaS3WriteAccess"
  description = "Allows Lambda to write proofing data to S3"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::metrosafety-bedrock-output-data-dev-bedrock-lambda",
          "arn:aws:s3:::metrosafety-bedrock-output-data-dev-bedrock-lambda/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  policy_arn = aws_iam_policy.lambda_s3_access.arn
  role       = "bedrock-lambda-salesforce_input"
}

# 1) Define a standalone policy that allows ListBucket + GetObject on "metrosafetyprodfiles"
resource "aws_iam_policy" "lambda_s3_read_metrosafetyprodfiles" {
  name        = "LambdaS3ReadMetroSafetyProdFiles"
  description = "Allow Lambda to ListBucket and GetObject on metrosafetyprodfiles"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "AllowListBucketOnProdFiles",
        Effect = "Allow",
        Action = [
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::metrosafetyprodfiles"
        ]
      },
      {
        Sid    = "AllowGetObjectsOnProdFiles",
        Effect = "Allow",
        Action = [
          "s3:GetObject"
        ],
        Resource = [
          "arn:aws:s3:::metrosafetyprodfiles/*"
        ]
      }
    ]
  })
}

# 2) Attach that policy to the same role your salesforce_input lambda uses:
resource "aws_iam_role_policy_attachment" "attach_s3_read_metrosafetyprod" {
  role       = "bedrock-lambda-salesforce_input"
  policy_arn = aws_iam_policy.lambda_s3_read_metrosafetyprodfiles.arn
}



#############################
# Unified Lambda Execution Role
#############################

resource "aws_iam_role" "bedrock_lambda_checklist" {
  name = "bedrock-lambda-checklist"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"

      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

#############################
# Textract Policy for Lambda
#############################

resource "aws_iam_policy" "lambda_textract_policy" {
  name   = "LambdaTextractPolicy"
  policy = jsonencode({
    Version   = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = [
          "textract:AnalyzeDocument",
          "textract:StartDocumentAnalysis",
          "textract:GetDocumentAnalysis"
        ],
        Resource = ["*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_textract_to_lambda" {
  role       = aws_iam_role.bedrock_lambda_checklist.name
  policy_arn = aws_iam_policy.lambda_textract_policy.arn
}

#############################
# S3 Read Policy for Lambda
#############################

data "aws_iam_policy_document" "lambda_s3_read" {
  statement {
    sid    = "AllowLambdaS3Read"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:HeadObject",
      "s3:ListBucket"
    ]
    resources = [
      "arn:aws:s3:::metrosafetyprodfiles",
      "arn:aws:s3:::metrosafetyprodfiles/*"
    ]
  }
}

resource "aws_iam_policy" "lambda_s3_read_policy" {
  name   = "LambdaS3ReadPolicy"
  policy = data.aws_iam_policy_document.lambda_s3_read.json
}

resource "aws_iam_role_policy_attachment" "attach_lambda_s3_read" {
  role       = aws_iam_role.bedrock_lambda_checklist.name
  policy_arn = aws_iam_policy.lambda_s3_read_policy.arn
}

resource "aws_sns_topic" "textract_topic" {
  name = "textract-job-notifications"
}

resource "aws_iam_role" "textract_service_role" {
  name = "TextractServiceRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "textract.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_policy" "textract_sns_policy" {
  name   = "TextractSNSPublishPolicy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = "sns:Publish",
        Resource = aws_sns_topic.textract_topic.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_textract_sns" {
  role       = aws_iam_role.textract_service_role.name
  policy_arn = aws_iam_policy.textract_sns_policy.arn
}

resource "aws_iam_policy" "bedrock_lambda_s3_policy" {
  name        = "bedrock-lambda-s3-policy"
  description = "Allows Lambda to put objects in the textract-output-digival bucket under the processed/ prefix."
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "AllowLambdaPutObject",
        "Effect": "Allow",
        "Action": "s3:PutObject",
        "Resource": "arn:aws:s3:::textract-output-digival/processed/*"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "bedrock_lambda_s3_policy_attachment" {
  name       = "bedrock-lambda-s3-policy-attachment"
  policy_arn = aws_iam_policy.bedrock_lambda_s3_policy.arn
  roles      = [ "bedrock-lambda-checklist" ]
}


###########
# 1. Proofing Lambda role
###########
resource "aws_iam_role" "bedrock_lambda_checklist_proofing" {
  name = "bedrock-lambda-checklist_proofing"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

###########
# 2. Basic execution (CloudWatch Logs)
###########
resource "aws_iam_role_policy_attachment" "proofing_basic_exec" {
  role       = aws_iam_role.bedrock_lambda_checklist_proofing.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

###########
# 3. Bedrock invocation
###########
resource "aws_iam_policy" "proofing_bedrock_invoke" {
  name        = "ProofingBedrockInvokePolicy"
  description = "Allow Lambda to invoke Bedrock models"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = "bedrock:InvokeModel",
      Resource = "arn:aws:bedrock:eu-west-2::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "proofing_bedrock_attach" {
  role       = aws_iam_role.bedrock_lambda_checklist_proofing.name
  policy_arn = aws_iam_policy.proofing_bedrock_invoke.arn
}

###########
# 4. S3 read
###########
data "aws_iam_policy_document" "proofing_s3_read" {
  statement {
    sid     = "AllowReadCsvBucket"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      "arn:aws:s3:::textract-output-digival",
      "arn:aws:s3:::textract-output-digival/*"    
]
  }
}

resource "aws_iam_policy" "proofing_s3_read_policy" {
  name   = "ProofingLambdaS3ReadPolicy"
  policy = data.aws_iam_policy_document.proofing_s3_read.json
}

resource "aws_iam_role_policy_attachment" "proofing_s3_read_attach" {
  role       = aws_iam_role.bedrock_lambda_checklist_proofing.name
  policy_arn = aws_iam_policy.proofing_s3_read_policy.arn
}

#
# 1) Policy to allow listing & getting objects under processed/*
#
resource "aws_iam_policy" "lambda_textract_output_read" {
  name        = "LambdaTextractOutputRead"
  description = "Allow proofing Lambda to read Textract output"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = "s3:ListBucket",
        Resource = "arn:aws:s3:::textract-output-digival"
      },
      {
        Effect   = "Allow",
        Action   = "s3:GetObject",
        Resource = "arn:aws:s3:::textract-output-digival/processed/*"
      }
    ]
  })
}

#
# 2) Attach it to your proofing Lambda’s execution role
#
resource "aws_iam_role_policy_attachment" "proofing_read_attach" {
  role       = aws_iam_role.bedrock_lambda_checklist_proofing.name
  policy_arn = aws_iam_policy.lambda_textract_output_read.arn
}

data "aws_iam_policy_document" "checklist_textract_read" {
  statement {
    sid    = "AllowReadTextractOutput"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetObject"
    ]
    resources = [
      "arn:aws:s3:::textract-output-digival",
      "arn:aws:s3:::textract-output-digival/*"
    ]
  }
}

resource "aws_iam_policy" "checklist_textract_read_policy" {
  name   = "ChecklistTextractReadPolicy"
  policy = data.aws_iam_policy_document.checklist_textract_read.json
}

resource "aws_iam_role_policy_attachment" "checklist_textract_read_attach" {
  role       = aws_iam_role.bedrock_lambda_checklist_proofing.name
  policy_arn = aws_iam_policy.checklist_textract_read_policy.arn
}

# -------------------------------------------------------------------
# 1) IAM policy allowing this role to invoke the proofing Lambda
# -------------------------------------------------------------------
data "aws_iam_role" "checklist_role" {
  name = "bedrock-lambda-checklist"
}

resource "aws_iam_role_policy" "allow_invoke_proofing" {
  name = "AllowInvokeChecklistProofing"
  role = data.aws_iam_role.checklist_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-checklist_proofing"
      }
    ]
  })
}

#
# 1) Look up the existing “bedrock-lambda-salesforce_input” role by name
#
data "aws_iam_role" "salesforce_input_role" {
  name = "bedrock-lambda-salesforce_input"
}

#
# 2) Attach a policy that allows ListBucket / HeadObject / PutObject
#    on the exact prefix where you create “.textract_ran”
#
resource "aws_iam_role_policy" "salesforce_input_s3_marker" {
  name = "AllowSalesforceInputToMarkTextractRan"
  role = data.aws_iam_role.salesforce_input_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # (a) allow listing “WorkOrders/<workOrderId>” so you can find the latest PDF
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = "arn:aws:s3:::metrosafetyprodfiles"
        Condition = {
          StringLike = {
            # only the WorkOrders/<workOrderId> prefix
            "s3:prefix" = "WorkOrders/*"
          }
        }
      },

      # (b) allow HeadObject so you can check for the marker
      {
        Effect = "Allow"
        Action = [
          "s3:HeadObject"
        ]
        Resource = "arn:aws:s3:::metrosafetyprodfiles/WorkOrders/*/.textract_ran"
      },

      # (c) allow putting the zero-byte marker “.textract_ran”
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = "arn:aws:s3:::metrosafetyprodfiles/WorkOrders/*/.textract_ran"
      }
    ]
  })
}

# 2) Attach a new inline policy that grants lambda:InvokeFunction on the checklist Lambda
resource "aws_iam_role_policy" "AllowSalesforceInput_Invoke_Checklist" {
  name = "AllowSalesforceInput_Invoke_Checklist"
  role = data.aws_iam_role.salesforce_input_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-checklist"
      }
    ]
  })
}

resource "aws_iam_role_policy" "allow_invoke_fra_checklist_proofing" {
  name = "AllowInvokeFRAProofing"
  role = aws_iam_role.bedrock_lambda_checklist.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-fra_checklist_proofing"
      }
    ]
  })
}

# look up the FRA‐proofing role by name
data "aws_iam_role" "fra_proofing_role" {
  name = "bedrock-lambda-fra_checklist_proofing"
}

# attach the same policy that allows GetObject on processed/*
resource "aws_iam_role_policy_attachment" "fra_textract_output_read" {
  role       = data.aws_iam_role.fra_proofing_role.name
  policy_arn = aws_iam_policy.lambda_textract_output_read.arn
}

# for your FRA proofing Lambda
resource "aws_iam_role_policy_attachment" "fra_bedrock_invoke" {
  role       = "bedrock-lambda-fra_checklist_proofing"
  policy_arn = aws_iam_policy.proofing_bedrock_invoke.arn
}

resource "aws_iam_role_policy" "allow_invoke_hsa_checklist_proofing" {
  name = "AllowInvokeHSAProofing"
  role = aws_iam_role.bedrock_lambda_checklist.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:eu-west-2:837329614132:function:bedrock-lambda-hsa_checklist_proofing"
      }
    ]
  })
}

# look up the HSA‐proofing role by name
data "aws_iam_role" "hsa_proofing_role" {
  name = "bedrock-lambda-hsa_checklist_proofing"
}

# attach the same policy that allows GetObject on processed/*
resource "aws_iam_role_policy_attachment" "hsa_textract_output_read" {
  role       = data.aws_iam_role.hsa_proofing_role.name
  policy_arn = aws_iam_policy.lambda_textract_output_read.arn
}

# for your HSA proofing Lambda
resource "aws_iam_role_policy_attachment" "hsa_bedrock_invoke" {
  role       = "bedrock-lambda-hsa_checklist_proofing"
  policy_arn = aws_iam_policy.proofing_bedrock_invoke.arn
}

resource "aws_iam_role_policy_attachment" "water_proofing_s3_read_metrosafetyprod" {
  role       = aws_iam_role.bedrock_lambda_checklist_proofing.name
  policy_arn = aws_iam_policy.lambda_s3_read_metrosafetyprodfiles.arn
}

resource "aws_iam_role_policy_attachment" "fra_s3_read_metrosafetyprod" {
  role       = "bedrock-lambda-fra_checklist_proofing"
  policy_arn = aws_iam_policy.lambda_s3_read_metrosafetyprodfiles.arn
}

resource "aws_iam_role_policy_attachment" "hsa_s3_read_metrosafetyprod" {
  role       = "bedrock-lambda-hsa_checklist_proofing"
  policy_arn = aws_iam_policy.lambda_s3_read_metrosafetyprodfiles.arn
}


# 1. Build the IAM policy document
data "aws_iam_policy_document" "lambda_s3_read_pabiltotesting" {
  statement {
    sid       = "AllowListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::pabiltotesting"]
  }
  statement {
    sid       = "AllowGetObject"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::pabiltotesting/*"]
  }
}

# 2. Create the IAM policy
resource "aws_iam_policy" "lambda_s3_read_pabiltotesting" {
  name        = "LambdaS3ReadPabilToTesting"
  description = "Allow Lambda to read objects from pabiltotesting bucket"
  policy      = data.aws_iam_policy_document.lambda_s3_read_pabiltotesting.json
}

# 3. Attach it to your Lambda’s execution role
resource "aws_iam_role_policy_attachment" "attach_s3_read_pabiltotesting" {
  role       = "bedrock-lambda-categorisation"              # or aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_read_pabiltotesting.arn
}