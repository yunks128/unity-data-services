import json

from cumulus_lambda_functions.granules_cnm_response_writer.cnm_result_writer import CnmResultWriter
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator


def lambda_handler(event, context):
    """
    :param event:
    :param context:
    :return:
    """
    LambdaLoggerGenerator.remove_default_handlers()
    CnmResultWriter().start(event)
    return
