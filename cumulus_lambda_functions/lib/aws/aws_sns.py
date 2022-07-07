from cumulus_lambda_functions.lib.aws.aws_cred import AwsCred


class AwsSns(AwsCred):
    def __init__(self):
        super().__init__()
        self.__sns_client = self.get_client('sns')
        self.__topic_arn = ''

    def set_topic_arn(self, topic_arn):
        self.__topic_arn = topic_arn
        return self

    def publish_message(self, msg_str: str):
        if self.__topic_arn == '':
            raise ValueError('missing topic arn to publish message')
        response = self.__sns_client.publish(
            TopicArn=self.__topic_arn,
            # TargetArn='string',  # not needed coz of we are using topic arn
            # PhoneNumber='string',  # not needed coz of we are using topic arn
            Message=msg_str,
            # Subject='optional string',
            # MessageStructure='string',
            # MessageAttributes={
            #     'string': {
            #         'DataType': 'string',
            #         'StringValue': 'string',
            #         'BinaryValue': b'bytes'
            #     }
            # },
            # MessageDeduplicationId='string',
            # MessageGroupId='string'
        )
        return response
