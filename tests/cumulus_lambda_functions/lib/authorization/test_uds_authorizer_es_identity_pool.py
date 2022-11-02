import os
from time import sleep
from unittest import TestCase

from cumulus_lambda_functions.lib.authorization.uds_authorizer_es_identity_pool import UDSAuthorizorEsIdentityPool


class TestUDSAuthorizorEsIdentityPool(TestCase):
    def test_01(self):
        os.environ['ES_URL'] = 'https://search-uds-es-test-2-olhpweojwudrginxdizzn3itt4.us-west-2.es.amazonaws.com'
        os.environ['AUTHORIZATION_INDEX'] = 'authorization_mappings'

        authorizer = UDSAuthorizorEsIdentityPool('us-west-2_FLDyXE2mO')

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
        os.environ['ES_URL'] = 'https://search-uds-es-test-2-olhpweojwudrginxdizzn3itt4.us-west-2.es.amazonaws.com'
        os.environ['AUTHORIZATION_INDEX'] = 'authorization_mappings'

        authorizer = UDSAuthorizorEsIdentityPool('us-west-2_FLDyXE2mO')
        authorizer.add_authorized_group(['PUT', 'POST'], ['COLLECTION'], 'unitty_project_1', 'DEV_001', 'Unity_Viewer')
        authorizer.add_authorized_group(['PUT'], ['COLLECTION', 'GRANULE'], 'unitty_project_2', 'DEV_001', 'Test_Group')
        sleep(2)
        self.assertEqual(len(authorizer.get_authorized_tenant('wphyo', 'GET', 'COLLECTION')), 0, f'wrong length of result')
        self.assertEqual(len(authorizer.get_authorized_tenant('wphyo', 'PUT', 'COLLECTION')), 2, f'wrong length of result')
        self.assertEqual(len(authorizer.get_authorized_tenant('wphyo', 'GET', 'GRANULE')), 0, f'wrong length of result')
        put_authorized_list = authorizer.get_authorized_tenant('wphyo', 'PUT', 'GRANULE')
        self.assertEqual(len(put_authorized_list), 1, f'wrong length of result')
        self.assertEqual(put_authorized_list[0]['tenant'], 'unitty_project_2', 'wrong project for PUT GRANULES')
        self.assertEqual(put_authorized_list[0]['tenant_venue'], 'DEV_001', 'wrong venue for PUT GRANULES')
        self.assertEqual(len(authorizer.get_authorized_tenant('wphyo', 'POST', 'COLLECTION')), 1, f'wrong length of result')
        self.assertEqual(len(authorizer.get_authorized_tenant('wphyo', 'DELETE', 'COLLECTION')), 0, f'wrong length of result')
        authorizer.delete_authorized_group('unitty_project_1', 'DEV_001', 'Unity_Viewer')
        authorizer.delete_authorized_group('unitty_project_2', 'DEV_001', 'Test_Group')

        return
