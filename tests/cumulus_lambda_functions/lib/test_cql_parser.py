import json
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

    def test_02(self):
        granule_ids = ['G1', 'G2', 'G3']
        granule_ids = [f"'{k}'" for k in granule_ids]
        filter_cql = f"id in ({','.join(granule_ids)}) and tags = 'level-3' and (time1 < 34 or time1 > 14)"
        parsed_result = CqlParser().transform(filter_cql)
        print(parsed_result)
        expected_result = {'bool': {'must': [{'bool': {'must': [{'terms': {'id': ['G1', 'G2', 'G3']}}, {'term': {'tags': 'level-3'}}]}}, {'bool': {'should': [{'range': {'time1': {'lt': 34}}}, {'range': {'time1': {'gt': 14}}}]}}]}}
        self.assertEqual(sorted(json.dumps(expected_result)), sorted(json.dumps(parsed_result)))
        return

    def test_03(self):
        granule_ids = ['G1', 'G2', 'G3']
        granule_ids = [f"'{k}'" for k in granule_ids]
        filter_cql = f"id in ({','.join(granule_ids)}) and tags::core = 'level-3' and (time1 < 34 or time1 > 14)"
        parsed_result = CqlParser().transform(filter_cql)
        print(parsed_result)
        expected_result = {'bool': {'must': [{'bool': {'must': [{'terms': {'id': ['G1', 'G2', 'G3']}}, {'term': {'tags::core': 'level-3'}}]}}, {'bool': {'should': [{'range': {'time1': {'lt': 34}}}, {'range': {'time1': {'gt': 14}}}]}}]}}
        self.assertEqual(sorted(json.dumps(expected_result)), sorted(json.dumps(parsed_result)))
        return
