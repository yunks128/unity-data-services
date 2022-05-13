import json
from unittest import TestCase

from cumulus_lambda_functions.cumulus_stac.collection_transformer import STAC_COLLECTION_SCHEMA
from cumulus_lambda_functions.lib.json_validator import JsonValidator


class TestItemTransformer(TestCase):
    def test_01(self):
        stac_validator = JsonValidator(json.loads(STAC_COLLECTION_SCHEMA))
        source = '''{
            "published": false,
            "endingDateTime": "2016-01-31T19:59:59.991043",
            "status": "completed",
            "timestamp": 1648050501578,
            "createdAt": 1648050499079,
            "processingEndDateTime": "2022-03-23T15:48:20.869Z",
            "productVolume": 18096656,
            "timeToPreprocess": 20.302,
            "timeToArchive": 0,
            "productionDateTime": "2016-02-01T02:45:59.639000Z",
            "execution": "https://console.aws.amazon.com/states/home?region=us-west-2#/executions/details/arn:aws:states:us-west-2:884500545225:execution:am-uds-dev-cumulus-IngestGranule:ec602ca7-0243-44df-adc0-28fb8a486d54",
            "files": [
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS",
                    "size": 760,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518500.PDS",
                    "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518500.PDS",
                    "type": "data"
                },
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518501.PDS",
                    "size": 18084600,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518501.PDS",
                    "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518501.PDS",
                    "type": "metadata"
                },
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518501.PDS.xml",
                    "size": 9547,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518501.PDS.xml",
                    "source": "data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2016/031//P1570515ATMSSCIENCEAAT16032024518501.PDS.xml",
                    "type": "metadata"
                },
                {
                    "bucket": "am-uds-dev-cumulus-internal",
                    "key": "ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml",
                    "size": 1749,
                    "fileName": "P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml",
                    "type": "metadata"
                }
            ],
            "processingStartDateTime": "2022-03-23T15:45:03.732Z",
            "updatedAt": 1648050501578,
            "beginningDateTime": "2016-01-31T18:00:00.009057",
            "provider": "snpp_provider_03",
            "granuleId": "P1570515ATMSSCIENCEAAT16032024518500.PDS",
            "collectionId": "ATMS_SCIENCE_Group___001",
            "duration": 197.993,
            "error": {},
            "lastUpdateDateTime": "2018-04-25T21:45:45.524053"
        }'''
        raw = {
            "type": "Collection",
            "stac_version": "1.0.0",
            # "stac_extensions": [],
            "id": "sentinel-2",
            "description": "Sentinel-2 is a wide-swath, high-resolution, multi-spectral\nimaging mission supporting Copernicus Land Monitoring studies,\nincluding the monitoring of vegetation, soil and water cover,\nas well as observation of inland waterways and coastal areas.\n\nThe Sentinel-2 data contain 13 UINT16 spectral bands representing\nTOA reflectance scaled by 10000. See the [Sentinel-2 User Handbook](https://sentinel.esa.int/documents/247904/685211/Sentinel-2_User_Handbook)\nfor details. In addition, three QA bands are present where one\n(QA60) is a bitmask band with cloud mask information. For more\ndetails, [see the full explanation of how cloud masks are computed.](https://sentinel.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-1c/cloud-masks)\n\nEach Sentinel-2 product (zip archive) may contain multiple\ngranules. Each granule becomes a separate Earth Engine asset.\nEE asset ids for Sentinel-2 assets have the following format:\nCOPERNICUS/S2/20151128T002653_20151128T102149_T56MNN. Here the\nfirst numeric part represents the sensing date and time, the\nsecond numeric part represents the product generation date and\ntime, and the final 6-character string is a unique granule identifier\nindicating its UTM grid reference (see [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System)).\n\nFor more details on Sentinel-2 radiometric resoltuon, [see this page](https://earth.esa.int/web/sentinel/user-guides/sentinel-2-msi/resolutions/radiometric).\n",
            "license": "proprietary",
            # "keywords": [],
            "providers": [],
            "extent": {
                "spatial": {
                    "bbox": [[0, 0, 0, 0]]
                },
                "temporal": {
                    "interval": [[None, None]]
                }
            },
            "assets": {},
            "summaries": {},
            "links": [
                {
                    "rel": "root",
                    "href": ".",
                },
            ]
        }
        self.assertEqual(None, stac_validator.validate(raw), f'invalid stac format: {stac_validator}')
        return
