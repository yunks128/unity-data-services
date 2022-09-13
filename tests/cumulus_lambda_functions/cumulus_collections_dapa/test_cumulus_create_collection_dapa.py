import json
import os
from datetime import datetime
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
        self.assertEqual(202, creation['statusCode'], f'wrong statusCode: {creation}')
        return

    def test_02(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ[Constants.PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.CLIENT_ID] = '7a1fglm2d54eoggj13lccivp25'  # JPL Cloud

        os.environ[Constants.COGNITO_URL] = 'https://cognito-idp.us-west-2.amazonaws.com'
        bearer_token = CognitoTokenRetriever().start()

        post_url = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa/collections/'
        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json',
        }

        dapa_collection = UnityCollectionStac() \
            .with_id(f'CUMULUS_DAPA_UNIT_TEST___{int(datetime.utcnow().timestamp())}') \
            .with_graule_id_regex("^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$") \
            .with_granule_id_extraction_regex("(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+") \
            .with_title("P1570515ATMSSCIENCEAXT11344000000001.PDS") \
            .with_process('modis') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS.cmr.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$",
                           'internal', 'data', 'item')
        stac_collection = dapa_collection.start()

        query_result = requests.post(url=post_url,
                                    headers=headers,
                                    json=stac_collection,
                                    )
        self.assertEqual(query_result.status_code, 202, f'wrong status code. {query_result.text}')
        return
