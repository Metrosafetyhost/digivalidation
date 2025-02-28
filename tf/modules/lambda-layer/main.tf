data "external" "git" {
  program = [
    "git",
    "log",
    "--pretty=format:{ \"sha\": \"%H\" }",
    "-1",
    "HEAD"
  ]
}

# path name root is the /tf directory
resource "aws_s3_object" "layer" {
  bucket = var.s3_zip_bucket
  key    = "${var.namespace}-layer.zip"
  source = abspath("../layer.zip")
  etag   = filemd5(abspath("../layer.zip"))

  metadata = {
    hash   = filebase64sha256(abspath("../layer.zip"))
    layer  = var.namespace
    commit = try(data.external.git.result["sha"], "null")
  }
}

resource "aws_lambda_layer_version" "layer" {
  description              = "Lambda Layer for ${var.namespace}"
  compatible_runtimes      = [var.runtime]
  compatible_architectures = [var.arch]
  s3_bucket                = var.s3_zip_bucket
  s3_key                   = aws_s3_object.layer.key
  layer_name               = "${var.namespace}-lambda-layer"

  source_code_hash = aws_s3_object.layer.metadata["commit"] == data.external.git.result["sha"] ? (
    var.force_layer_code_deploy ? aws_s3_object.layer.metadata["hash"] : null
  ) : aws_s3_object.layer.metadata["hash"]
}
