resource "aws_lambda_layer_version" "llamaindex" {
  layer_name               = "llama-py-312-x86_64"   # rename for clarity
  s3_bucket                = module.lambda_zips_bucket.this_s3_bucket_id
  s3_key                   = "layers/llama-layer.zip"
  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["x86_64"]

  # force a new layer version whenever you upload a new zip
  description = "llama layer build ${timestamp()}"
}