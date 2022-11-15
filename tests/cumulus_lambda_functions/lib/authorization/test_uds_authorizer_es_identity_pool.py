import os
from time import sleep
from unittest import TestCase

from cumulus_lambda_functions.lib.authorization.uds_authorizer_es_identity_pool import UDSAuthorizorEsIdentityPool
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants


class TestUDSAuthorizorEsIdentityPool(TestCase):
    def test_01(self):
        es_url = 'https://search-uds-es-test-2-olhpweojwudrginxdizzn3itt4.us-west-2.es.amazonaws.com'
        authorizer = UDSAuthorizorEsIdentityPool('us-west-2_FLDyXE2mO', es_url)

        authorizer.add_authorized_group(['PUT', 'POST'], ['COLLECTION'], 'unitty_project_1', 'DEV_001', 'sample_group_1A')
        authorizer.add_authorized_group(['PUT', 'POST'], ['COLLECTION', 'GET'], 'unitty_project_2', 'DEV_001', 'sample_group_2A')
        authorizer.add_authorized_group(['GET'], ['COLLECTION', 'GET'], 'unitty_project_1', 'DEV_001', 'sample_group_1B')
        authorizer.add_authorized_group(['DELETE'], ['COLLECTION'], 'unitty_project_1', 'DEV_001', 'sample_group_1C')
        sleep(2)
        authorized_groups = authorizer.list_authorized_groups_for('unitty_project_1', 'DEV_001')
        self.assertEqual(len(authorized_groups), 3, f'invalid length of result')
        authorizer.delete_authorized_group('unitty_project_1', 'DEV_001', 'sample_group_1A')
        authorizer.delete_authorized_group('unitty_project_1', 'DEV_001', 'sample_group_1B')
        authorizer.delete_authorized_group('unitty_project_1', 'DEV_001', 'sample_group_1C')
        authorizer.delete_authorized_group('unitty_project_2', 'DEV_001', 'sample_group_2A')
        sleep(2)
        authorized_groups = authorizer.list_authorized_groups_for('unitty_project_1', 'DEV_001')
        self.assertEqual(len(authorized_groups), 0, f'invalid length of result')
        authorizer.update_authorized_group(['PUT', 'POST'], ['COLLECTION'], 'unitty_project_1', 'DEV_001', 'sample_group_1A')
        sleep(2)
        authorized_groups = authorizer.list_authorized_groups_for('unitty_project_1', 'DEV_001')
        self.assertEqual(len(authorized_groups), 1, f'invalid length of result')
        return

    def test_02(self):
        es_url = 'https://search-uds-es-test-2-olhpweojwudrginxdizzn3itt4.us-west-2.es.amazonaws.com'
        authorizer = UDSAuthorizorEsIdentityPool('us-west-2_FLDyXE2mO', es_url)
        authorizer.add_authorized_group([DBConstants.create, DBConstants.update], ['urn:nasa:unity:unitty_project_1:DEV_001:ecm_*', 'urn:nasa:unity:unitty_project_1:DEV_001:aaa_*'], 'unitty_project_1', 'DEV_001', 'Unity_Viewer')
        authorizer.add_authorized_group([DBConstants.create], ['urn:nasa:unity:unitty_project_2:DEV_001:ecm_*', 'urn:nasa:unity:unitty_project_2:DEV_001:ls_*'], 'unitty_project_2', 'DEV_001', 'Test_Group')
        sleep(2)
        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.create,
                                                                'urn:nasa:unity:unitty_project_1:DEV_001:ecm_ids__001',
                                                                'wphyo', 'unitty_project_1', 'DEV_001'))
        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.update,
                                                                'urn:nasa:unity:unitty_project_1:DEV_001:ecm_ids__001',
                                                                'wphyo', 'unitty_project_1', 'DEV_001'))
        self.assertFalse(authorizer.is_authorized_for_collection(DBConstants.update,
                                                                 'urn:nasa:unity:unitty_project_1:DEV_001:ecn_ids__001',
                                                                 'wphyo', 'unitty_project_1', 'DEV_001'))

        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.create,
                                                                'urn:nasa:unity:unitty_project_2:DEV_001:ecm_ids__001',
                                                                'wphyo', 'unitty_project_2', 'DEV_001'))
        self.assertTrue(authorizer.is_authorized_for_collection(DBConstants.create,
                                                                'urn:nasa:unity:unitty_project_2:DEV_001:ls_ids__001',
                                                                'wphyo', 'unitty_project_2', 'DEV_001'))
        self.assertFalse(authorizer.is_authorized_for_collection(DBConstants.update,
                                                                 'urn:nasa:unity:unitty_project_2:DEV_001:ls_ids__001',
                                                                 'wphyo', 'unitty_project_2', 'DEV_001'))

        collection_regex = authorizer.get_authorized_collections(DBConstants.create, 'wphyo')
        self.assertEqual(4, len(collection_regex), f'wrong length: {collection_regex}')

        collection_regex = authorizer.get_authorized_collections(DBConstants.update, 'wphyo')
        self.assertEqual(2, len(collection_regex), f'wrong length: {collection_regex}')

        authorizer.delete_authorized_group('unitty_project_1', 'DEV_001', 'Unity_Viewer')
        authorizer.delete_authorized_group('unitty_project_2', 'DEV_001', 'Test_Group')
        return
