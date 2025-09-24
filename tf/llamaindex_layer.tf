resource "aws_lambda_layer_version" "llamaindex" {
  layer_name               = "llama-py-313-x86_64"
  s3_bucket                = module.lambda_zips_bucket.this_s3_bucket_id
  s3_key                   = "layers/llama-layer.zip"
  compatible_runtimes      = ["python3.12"]
  compatible_architectures = ["x86_64"]
}
