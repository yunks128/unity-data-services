from unittest import TestCase

from cumulus_lambda_functions.lib.aws.security_manager_middleware import SecurityManagerMiddleware


class TestSecurityManagerMiddleware(TestCase):
    def test_get_secret(self):
        ssm = SecurityManagerMiddleware()
        result = ssm.get_secret('am-uds-dev-cumulus_db_login20220204064442438900000001')
        self.assertTrue(isinstance(result, dict), 'result is not dictionary')
        return
