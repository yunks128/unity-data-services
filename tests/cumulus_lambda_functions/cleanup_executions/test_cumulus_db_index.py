import os
from unittest import TestCase

from cumulus_lambda_functions.lib.time_utils import TimeUtils

from cumulus_lambda_functions.cleanup_executions.cumulus_db_index import CumulusDbIndex


class TestCumulusDbIndex(TestCase):
    def test_01(self):
        os.environ['ES_URL'] = 'https://vpc-uds-dev-cumulus-es-vpc-hjnnwrivoe36fiak4eomtgqlvq.us-west-2.es.amazonaws.com'
        os.environ['ES_PORT'] = '9200'
        cumulus_db = CumulusDbIndex()
        time1 = int(TimeUtils().get_unix_from_timestamp('2024-001T00:00:00.000') * 1000)
        result = cumulus_db.delete_executions(time1)
        print(result)
        self.assertTrue('deleted' in result, f'wrong response: {result}')
        return
