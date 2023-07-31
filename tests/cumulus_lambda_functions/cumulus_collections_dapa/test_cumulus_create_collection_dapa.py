import json
import os
from datetime import datetime
from time import sleep
from unittest import TestCase

import requests

from cumulus_lambda_functions.cumulus_collections_dapa.cumulus_create_collection_dapa import CumulusCreateCollectionDapa
from cumulus_lambda_functions.cumulus_stac.unity_collection_stac import UnityCollectionStac
from cumulus_lambda_functions.lib.cognito_login.cognito_token_retriever import CognitoTokenRetriever
from cumulus_lambda_functions.lib.constants import Constants


class TestCumulusCreateCollectionDapa(TestCase):
    def test_01(self):
        dapa_collection = UnityCollectionStac() \
            .with_id(f'CUMULUS_DAPA_UNIT_TEST___{int(datetime.utcnow().timestamp())}') \
            .with_graule_id_regex("^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$") \
            .with_granule_id_extraction_regex("(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+") \
            .with_title("P1570515ATMSSCIENCEAXT11344000000001.PDS") \
            .with_process('modis') \
            .with_provider('Test123')\
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS.cmr.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$",
                           'internal', 'data', 'item')
        os.environ['CUMULUS_LAMBDA_PREFIX'] = 'am-uds-dev-cumulus'
        os.environ['CUMULUS_WORKFLOW_SQS_URL'] = 'https://sqs.us-west-2.amazonaws.com/884500545225/am-uds-dev-cumulus-cnm-submission-queue'
        stac_collection = dapa_collection.start()
        event = {
            'body': json.dumps(stac_collection)
        }
        creation = CumulusCreateCollectionDapa(event).start()
        self.assertTrue('statusCode' in creation, f'missing statusCode: {creation}')
        self.assertEqual(200, creation['statusCode'], f'wrong statusCode: {creation}')
        return

    def test_get(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        # os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # JPL Cloud
        post_url = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # MCP Dev
        headers = {
            'Authorization': f'Bearer {bearer_token}',
        }

        query_result = requests.get(url=post_url,
                                    headers=headers,
                                    )
        self.assertEqual(query_result.status_code, 200, f'wrong status code. {query_result.text}')
        return

    def test_02(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'  # JPL Cloud
        post_url = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections'  # MCP Dev
        headers = {
            'Authorization': f'Bearer eyJraWQiOiJsWmw3XC9yYXFVTVRaTHBVMnJ3bm1paXZKSCtpVFlONngxSUhQNndZaU03RT0iLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI4MjJiNmQwYy05MDU0LTRjNDMtYTkwZS04YjU5YjI2MTZiMzUiLCJjb2duaXRvOmdyb3VwcyI6WyJVbml0eV9WaWV3ZXIiLCJVbml0eV9BZG1pbiJdLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtd2VzdC0yLmFtYXpvbmF3cy5jb21cL3VzLXdlc3QtMl95YU93M3lqMHoiLCJjbGllbnRfaWQiOiI3MWcwYzczamw3N2dzcWh0bGZnMmh0Mzg4YyIsIm9yaWdpbl9qdGkiOiI2NDQ3NGU2Mi1hNDQxLTQyOTctYjFiMC1iMWQ4N2YxZjBkMTUiLCJldmVudF9pZCI6Ijg2NzMxMzliLWQ4YzgtNDI0Zi1hM2QzLWE0NWY1OGJiZGI4ZCIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4iLCJhdXRoX3RpbWUiOjE2OTAzOTc2NjQsImV4cCI6MTY5MDQwMTI2NCwiaWF0IjoxNjkwMzk3NjY0LCJqdGkiOiIzZDFhZTQ3My1iNmEyLTQ5YzctYWZhNy0wMzE2MGQwOGE0ZmMiLCJ1c2VybmFtZSI6IndwaHlvIn0.FokPAexC8G1Ivpbh9jHIq7CEYYeHmeMg-Qt8EavcuH6NYXaQvyNZZ9DpHGJIJXhTyEXexVkYgGIZBcKz7Vm2jFMfOsMbjcVkUMwgeO7iDsCcg6iGSZvkWd0TK5eFCWCzi8vdq5oZ62zqvU4QmPd4eDF5zBTjmSRZ_8m4ufUy1z9bHVdI6NGK8yCqAUil3Ek6cEhaV8bjSdhaRCJPHMOT-UYSKDm4ZJ1Q6xqr-tnmc5ZyUslolIcTwZk5MXFOFB125RSaGRg7aoiXg5K175w7vZxmOnrJ7v365Y7jfYacSVuvcAwY7HMWVCOFmFvnluEdX4FaKTVTsOGGaknPE5tn7g',
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
        collection_created_result = requests.get(url=f'{post_url}{temp_collection_id}', headers=headers)
        self.assertEqual(collection_created_result.status_code, 200, f'wrong status code. {collection_created_result.text}')
        collection_created_result = json.loads(collection_created_result.text)
        self.assertTrue('features' in collection_created_result, f'features not in collection_created_result: {collection_created_result}')
        self.assertEqual(len(collection_created_result['features']), 1, f'wrong length: {collection_created_result}')
        self.assertEqual(collection_created_result['features'][0]['id'], temp_collection_id, f'wrong id')
        # TODO check if collection shows up
        return
