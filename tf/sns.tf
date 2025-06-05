# resource "aws_sns_topic" "textract_topic" {
#   name = "textract-job-notifications"
# }

# resource "aws_iam_role" "textract_service_role" {
#   name = "TextractServiceRole"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [{
#       Effect = "Allow",
#       Principal = { Service = "textract.amazonaws.com" },
#       Action = "sts:AssumeRole"
#     }]
#   })
# }

# resource "aws_iam_policy" "textract_sns_policy" {
#   name   = "TextractSNSPublishPolicy"
#   policy = jsonencode({
#     Version   = "2012-10-17",
#     Statement = [
#       {
#         Effect   = "Allow",
#         Action   = "sns:Publish",
#         Resource = aws_sns_topic.textract_topic.arn
#       }
#     ]
#   })
# }

# resource "aws_iam_role_policy_attachment" "attach_textract_sns" {
#   role       = aws_iam_role.textract_service_role.name
#   policy_arn = aws_iam_policy.textract_sns_policy.arn
# }
