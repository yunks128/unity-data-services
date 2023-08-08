from unittest import TestCase

from cumulus_lambda_functions.lib.aws.aws_cognito import AwsCognito


class TestAwsCognitor(TestCase):
    def test_01(self):
        cognito = AwsCognito('us-west-2_FLDyXE2mO')
        wphyo_groups = cognito.get_groups('wphyo')
        self.assertTrue(isinstance(wphyo_groups, list), f'response is not list. {wphyo_groups}')
        self.assertTrue(len(wphyo_groups) > 0, f'empty list')
        print(wphyo_groups)
        return
