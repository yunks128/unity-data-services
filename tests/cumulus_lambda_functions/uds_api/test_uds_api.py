import json
import os
from datetime import datetime
from time import sleep
from unittest import TestCase

import requests

from cumulus_lambda_functions.cumulus_stac.unity_collection_stac import UnityCollectionStac
from cumulus_lambda_functions.lib.cognito_login.cognito_token_retriever import CognitoTokenRetriever
from cumulus_lambda_functions.lib.constants import Constants


class TestCumulusCreateCollectionDapa(TestCase):

    def test_collections_get(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        # os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev
        # os.environ[Constants.CLIENT_ID] = '6ir9qveln397i0inh9pmsabq1'  # MCP Test

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # JPL Cloud
        post_url = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # MCP Dev
        # post_url = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/'  # MCP Dev
        headers = {
            'Authorization': f'Bearer {bearer_token}',
        }

        query_result = requests.get(url=post_url,
                                    headers=headers,
                                    )
        self.assertEqual(query_result.status_code, 200, f'wrong status code. {query_result.text}')
        return

    def test_granules_get(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        # os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev
        # os.environ[Constants.CLIENT_ID] = '6ir9qveln397i0inh9pmsabq1'  # MCP Test

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/L0_SNPP_ATMS_SCIENCE___1/items/'  # JPL Cloud
        post_url = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/L0_SNPP_ATMS_SCIENCE___1/items/'  # MCP Dev
        # post_url = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/L0_SNPP_ATMS_SCIENCE___1/items/'  # MCP Dev
        headers = {
            'Authorization': f'Bearer {bearer_token}',
        }

        query_result = requests.get(url=post_url,
                                    headers=headers,
                                    )
        self.assertEqual(query_result.status_code, 200, f'wrong status code. {query_result.text}')
        return

    def test_create_new_collection(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # JPL Cloud
        post_url = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # MCP Dev
        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json',
        }
        temp_collection_id = f'CUMULUS_DAPA_UNIT_TEST___{int(datetime.utcnow().timestamp())}'
        dapa_collection = UnityCollectionStac() \
            .with_id(temp_collection_id) \
            .with_graule_id_regex("^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$") \
            .with_granule_id_extraction_regex("(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+") \
            .with_title("P1570515ATMSSCIENCEAXT11344000000001.PDS") \
            .with_process('modis') \
            .with_provider('unity') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01.PDS$", 'internal', 'metadata', 'root') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS.cmr.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$",
                           'internal', 'data', 'item')
        print(dapa_collection)
        stac_collection = dapa_collection.start()
        print(json.dumps(stac_collection))
        query_result = requests.post(url=post_url,
                                    headers=headers,
                                    json=stac_collection,
                                    )
        self.assertEqual(query_result.status_code, 202, f'wrong status code. {query_result.text}')
        sleep(60)
        post_url = post_url if post_url.endswith('/') else f'{post_url}/'
        collection_created_result = requests.get(url=f'{post_url}{temp_collection_id}', headers=headers)
        self.assertEqual(collection_created_result.status_code, 200, f'wrong status code. {collection_created_result.text}')
        collection_created_result = json.loads(collection_created_result.text)
        self.assertTrue('features' in collection_created_result, f'features not in collection_created_result: {collection_created_result}')
        self.assertEqual(len(collection_created_result['features']), 1, f'wrong length: {collection_created_result}')
        self.assertEqual(collection_created_result['features'][0]['id'], temp_collection_id, f'wrong id')
        # TODO check if collection shows up
        return

    def test_cnm_facade(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        # os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev
        # os.environ[Constants.CLIENT_ID] = '6ir9qveln397i0inh9pmsabq1'  # MCP Test

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # JPL Cloud
        post_url = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # MCP Dev
        # post_url = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/'  # MCP Dev
        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json',
        }
        stac_collection = {
            "provider_id": 'unity',
            "features": [
            {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'NEW_COLLECTION_EXAMPLE_L1B___9:test_file01',
             'properties': {'start_datetime': '2016-01-31T18:00:00.009057Z', 'end_datetime': '2016-01-31T19:59:59.991043Z',
                            'created': '2016-02-01T02:45:59.639000Z', 'updated': '2022-03-23T15:48:21.578000Z',
                            'datetime': '1970-01-01T00:00:00Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]},
             'links': [], 'assets': {
                'data': {'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc',
                         'title': 'main data'}, 'metadata__cas': {
                    'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.cas',
                    'title': 'metadata cas'}, 'metadata__stac': {
                    'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.stac.json',
                    'title': 'metadata stac'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [],
             'collection': 'NEW_COLLECTION_EXAMPLE_L1B___9'}]
        }
        query_result = requests.put(url=post_url,
                                    headers=headers,
                                    json=stac_collection,
                                    )
        self.assertEqual(query_result.status_code, 202, f'wrong status code. {query_result.text}')
        return
