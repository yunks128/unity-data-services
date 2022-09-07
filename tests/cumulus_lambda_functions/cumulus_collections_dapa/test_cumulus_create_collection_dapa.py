import json
import os
from datetime import datetime
from unittest import TestCase

from cumulus_lambda_functions.cumulus_collections_dapa.cumulus_create_collection_dapa import CumulusCreateCollectionDapa
from cumulus_lambda_functions.cumulus_stac.unity_collection_stac import UnityCollectionStac


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
        stac_collection = dapa_collection.start()
        event = {
            'body': json.dumps(stac_collection)
        }
        creation = CumulusCreateCollectionDapa(event).start()
        self.assertTrue('statusCode' in creation, f'missing statusCode: {creation}')
        self.assertEqual(200, creation['statusCode'], f'wrong statusCode: {creation}')
        return
