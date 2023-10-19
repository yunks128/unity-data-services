from unittest import TestCase

from cumulus_lambda_functions.lib.cql_parser import CqlParser


class TestCqlParser(TestCase):
    def test_01(self):
        granule_ids = ['G1', 'G2', 'G3']
        granule_ids = [f"'{k}'" for k in granule_ids]
        filter_cql = f"id in ({','.join(granule_ids)})"
        parsed_result = CqlParser().transform(filter_cql)
        expected_result = {'terms': {'id': ['G1', 'G2', 'G3']}}
        self.assertEqual(expected_result, parsed_result)
        return
