from cumulus_lambda_functions.lib.json_validator import JsonValidator

from cumulus_lambda_functions.lib.aws.aws_message_transformers import AwsMessageTransformers


class GranulesIndexer:
    CUMULUS_SCHEMA = {
        'type': 'object',
        'required': ['event', 'record'],
        'properties': {
            'event': {'type': 'string'},
            'record': {'type': 'object'},
        }
    }

    def __init__(self, event) -> None:
        self.__event = event

    def start(self):
        incoming_msg = AwsMessageTransformers().sqs_sns(self.__event)
        result = JsonValidator(self.CUMULUS_SCHEMA).validate(incoming_msg)
        if result is not None:
            raise ValueError(f'input json has CUMULUS validation errors: {result}')
        cumulus_record = incoming_msg['record']
        if len(cumulus_record['files']) < 1:
            # TODO ingest updating stage?
            return

        return self
