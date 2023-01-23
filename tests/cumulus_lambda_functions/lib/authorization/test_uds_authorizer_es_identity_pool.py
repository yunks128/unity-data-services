import os
from datetime import datetime
from time import sleep
from unittest import TestCase

from cumulus_lambda_functions.lib.authorization.uds_authorizer_es_identity_pool import UDSAuthorizorEsIdentityPool
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants


class TestUDSAuthorizorEsIdentityPool(TestCase):
    def test_01(self):
        es_url = 'https://search-uds-es-test-2-olhpweojwudrginxdizzn3itt4.us-west-2.es.amazonaws.com'
        authorizer = UDSAuthorizorEsIdentityPool(es_url)
        temp_venue = f'TEST_{int(datetime.utcnow().timestamp())}'
        authorizer.add_authorized_group(['PUT', 'POST'], ['COLLECTION'], 'unitty_project_1', temp_venue, 'sample_group_1A')
        authorizer.add_authorized_group(['PUT', 'POST'], ['COLLECTION', 'GET'], 'unitty_project_2', temp_venue, 'sample_group_2A')
        authorizer.add_authorized_group(['GET'], ['COLLECTION', 'GET'], 'unitty_project_1', temp_venue, 'sample_group_1B')
        authorizer.add_authorized_group(['DELETE'], ['COLLECTION'], 'unitty_project_1', temp_venue, 'sample_group_1C')
        sleep(2)
        authorized_groups = authorizer.list_authorized_groups_for('unitty_project_1', temp_venue)
        self.assertEqual(len(authorized_groups), 3, f'invalid length of result')
        authorizer.delete_authorized_group('unitty_project_1', temp_venue, 'sample_group_1A')
        authorizer.delete_authorized_group('unitty_project_1', temp_venue, 'sample_group_1B')
        authorizer.delete_authorized_group('unitty_project_1', temp_venue, 'sample_group_1C')
        authorizer.delete_authorized_group('unitty_project_2', temp_venue, 'sample_group_2A')
        sleep(2)
        authorized_groups = authorizer.list_authorized_groups_for('unitty_project_1', temp_venue)
        self.assertEqual(len(authorized_groups), 0, f'invalid length of result')
        authorizer.update_authorized_group(['PUT', 'POST'], ['COLLECTION'], 'unitty_project_1', temp_venue, 'sample_group_1A')
        sleep(2)
        authorized_groups = authorizer.list_authorized_groups_for('unitty_project_1', temp_venue)
        self.assertEqual(len(authorized_groups), 1, f'invalid length of result')
        return

    def test_02(self):
        temp_venue = f'TEST_{int(datetime.utcnow().timestamp())}'
        es_url = 'https://search-uds-es-test-2-olhpweojwudrginxdizzn3itt4.us-west-2.es.amazonaws.com'
        authorizer = UDSAuthorizorEsIdentityPool(es_url)
        authorizer.add_authorized_group([DBConstants.create, DBConstants.update], [f'urn:nasa:unity:unitty_project_1:{temp_venue}:ecm_*', f'urn:nasa:unity:unitty_project_1:{temp_venue}:aaa_*'], 'unitty_project_1', temp_venue, 'Unity_Viewer')
        authorizer.add_authorized_group([DBConstants.create], [f'urn:nasa:unity:unitty_project_2:{temp_venue}:ecm_*', f'urn:nasa:unity:unitty_project_2:{temp_venue}:ls_*'], 'unitty_project_2', temp_venue, 'Test_Group')
        sleep(2)
        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.create,
                                                                f'urn:nasa:unity:unitty_project_1:{temp_venue}:ecm_ids__001',
                                                                ['Unity_Viewer', 'Test_Group'], 'unitty_project_1', temp_venue))
        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.update,
                                                                f'urn:nasa:unity:unitty_project_1:{temp_venue}:ecm_ids__001',
                                                                ['Unity_Viewer', 'Test_Group'], 'unitty_project_1', temp_venue))
        self.assertFalse(authorizer.is_authorized_for_collection(DBConstants.update,
                                                                 f'urn:nasa:unity:unitty_project_1:{temp_venue}:ecn_ids__001',
                                                                 ['Unity_Viewer', 'Test_Group'], 'unitty_project_1', temp_venue))

        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.create,
                                                                f'urn:nasa:unity:unitty_project_2:{temp_venue}:ecm_ids__001',
                                                                ['Unity_Viewer', 'Test_Group'], 'unitty_project_2', temp_venue))
        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.create,
                                                                f'urn:nasa:unity:unitty_project_2:{temp_venue}:ls_ids__001',
                                                                ['Unity_Viewer', 'Test_Group'], 'unitty_project_2', temp_venue))
        self.assertFalse(authorizer.is_authorized_for_collection(DBConstants.update,
                                                                 f'urn:nasa:unity:unitty_project_2:{temp_venue}:ls_ids__001',
                                                                 ['Unity_Viewer', 'Test_Group'], 'unitty_project_2', temp_venue))

        collection_regex = authorizer.get_authorized_collections(DBConstants.create, ['Unity_Viewer', 'Test_Group'])
        collection_regex = set(collection_regex)
        for each_regex in [f'urn:nasa:unity:unitty_project_1:{temp_venue}:ecm_*', f'urn:nasa:unity:unitty_project_1:{temp_venue}:aaa_*', f'urn:nasa:unity:unitty_project_2:{temp_venue}:ecm_*', f'urn:nasa:unity:unitty_project_2:{temp_venue}:ls_*']:
            self.assertTrue(each_regex in collection_regex, f'missing regex: {each_regex}')

        collection_regex = authorizer.get_authorized_collections(DBConstants.update, ['Unity_Viewer'])
        collection_regex = set(collection_regex)
        for each_regex in [f'urn:nasa:unity:unitty_project_1:{temp_venue}:ecm_*',
                           f'urn:nasa:unity:unitty_project_1:{temp_venue}:aaa_*']:
            self.assertTrue(each_regex in collection_regex, f'missing regex: {each_regex}')

        authorizer.delete_authorized_group('unitty_project_1', temp_venue, 'Unity_Viewer')
        authorizer.delete_authorized_group('unitty_project_2', temp_venue, 'Test_Group')
        return
