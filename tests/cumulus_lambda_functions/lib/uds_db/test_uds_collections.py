import uuid
from unittest import TestCase

from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections


class TestUdsCollections(TestCase):
    def test_01(self):
        uds_collection = UdsCollections('https://search-uds-local-sample-iuthdh4e3yqre3mcfjtlfmxbua.us-west-2.es.amazonaws.com', 443)
        collection_prefix = f'urn:nasa:unity:unitty_project_1:DEV_001:ecm_{uuid.uuid4()}'
        collection_id = f'{collection_prefix}_test_1___1'
        uds_collection.add_collection(collection_id, 0, 10, [0.1, 1.2, 2.3, 3.4], 12)
        collections = uds_collection.get_collections([f'{collection_prefix}.*'])
        self.assertEqual(1, len(collections))
        uds_collection.delete_collection(collection_id)
        collections = uds_collection.get_collections([f'{collection_prefix}.*'])
        self.assertEqual(0, len(collections))
        return
