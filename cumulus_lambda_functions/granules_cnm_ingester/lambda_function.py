import json
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator


def lambda_handler(event, context):
    """
    :param event:
    :param context:
    :return:
    """
    LambdaLoggerGenerator.remove_default_handlers()
    print(f'event: {event}')
    raise NotImplementedError('Require implementation later')
