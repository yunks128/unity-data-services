from cumulus_lambda_functions.lib.aws.aws_cred import AwsCred


class SecurityManagerMiddleware(AwsCred):
    def __init__(self):
        super().__init__()
        self.__ssm = self.get_client('secretsmanager')

    def get_secret(self, secret_id: str):
        """
        ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager.html#SecretsManager.Client.get_secret_value
        sample response:

        {
    'ARN': 'string',
    'Name': 'string',
    'VersionId': 'string',
    'SecretBinary': b'bytes',
    'SecretString': 'string',
    'VersionStages': [
        'string',
    ],
    'CreatedDate': datetime(2015, 1, 1)
}

        :param secret_id:
        :return:
        """
        response = self.__ssm.get_secret_value(
            SecretId=secret_id,
            # VersionId='string',
            # VersionStage='string'
        )
        return response['SecretString']
