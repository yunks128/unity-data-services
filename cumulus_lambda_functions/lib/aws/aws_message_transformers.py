import json

from cumulus_lambda_functions.lib.json_validator import JsonValidator


class AwsMessageTransformers:
    SQS_SCHEMA = {
        'type': 'object',
        'properties': {
            'Records': {
                'type': 'array',
                'minItems': 1,
                'maxItems': 1,
                'items': {
                    'type': 'object',
                    'properties': {
                        'body': {'type': 'string', 'minLength': 1}
                    },
                    'required': ['body']
                }
            }
        },
        'required': ['Records']
    }

    SNS_SCHEMA = {
        "type": "object",
        "properties": {
            "Type": {"type": "string"},
            "MessageId": {"type": "string"},
            "TopicArn": {"type": "string"},
            "Message": {"type": "string"},
        }
    }

    S3_RECORD_SCHEMA = {
        'type': 'object',
        'properties': {'Records': {
            'type': 'array',
            'minItems': 1,
            'maxItems': 1,
            'items': {
                'type': 'object',
                'properties': {'s3': {
                    'type': 'object',
                    'properties': {
                        'bucket': {
                            'type': 'object',
                            'properties': {'name': {'type': 'string', 'minLength': 1}},
                            'required': ['name']
                        },
                        'object': {
                            'type': 'object',
                            'properties': {'key': {'type': 'string', 'minLength': 1}},
                            'required': ['key']
                        }},
                    'required': ['bucket', 'object']
                }},
                'required': ['s3']
            }
        }},
        'required': ['Records']
    }

    def sqs_sns(self, raw_msg: json):
        result = JsonValidator(self.SQS_SCHEMA).validate(raw_msg)
        if result is not None:
            raise ValueError(f'input json has SQS validation errors: {result}')
        sqs_msg_body = raw_msg['Records'][0]['body']
        sqs_msg_body = json.loads(sqs_msg_body)
        result = JsonValidator(self.SNS_SCHEMA).validate(sqs_msg_body)
        if result is not None:
            raise ValueError(f'input json has SNS validation errors: {result}')
        sns_msg_body = sqs_msg_body['Message']
        sns_msg_body = json.loads(sns_msg_body)
        return sns_msg_body
