from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.logging import correlation_paths

logger = Logger()

# @trove_default_decorators(log_event=True,correlation_id_path=correlation_paths.API_GATEWAY_REST)
@logger.inject_lambda_context() #(correlation_id_path=correlation_paths.API_GATEWAY_REST)
def process(event: dict, context: LambdaContext) -> str:
    logger.debug(f"Correlation ID => {logger.get_correlation_id()}")
    logger.info("Logging event")

    return "hola mundo"
