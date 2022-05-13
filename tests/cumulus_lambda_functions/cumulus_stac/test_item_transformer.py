import json
from unittest import TestCase

from cumulus_lambda_functions.cumulus_stac.item_transformer import ItemTransformer, STAC_ITEM_SCHEMA
from cumulus_lambda_functions.lib.json_validator import JsonValidator


class TestItemTransformer(TestCase):
    def test_01(self):
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
            "stac_version": "1.0.0",
            "stac_extensions": [],
            "type": "Feature",
            "id": "20201211_223832_CS2",
            "bbox": [
                172.91173669923782,
                1.3438851951615003,
                172.95469614953714,
                1.3690476620161975
            ],
            "geometry": {
    "type": "Point",
    "coordinates": [ 0,0]  },
            "properties": {
                "datetime": "2020-12-11T22:38:32.125000Z"
            },
            "collection": "simple-collection",
            "links": [
                {
                    "rel": "collection",
                    "href": ".",
                }
            ],
            "assets": {
                "data": {
                    "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "3-Band Visual",
                    "roles": [
                        "visual"
                    ]
                },
                "thumbnail": {
                    "href": "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.jpg",
                    "title": "Thumbnail",
                    "type": "image/jpeg",
                    "roles": [
                        "thumbnail"
                    ]
                }
            }
        }
        source = json.loads(source)
        stac_item = ItemTransformer().to_stac(source)
        sample_stac_item = {'stac_version': '1.0.0', 'stac_extensions': [], 'type': 'Feature',
                            'id': 'P1570515ATMSSCIENCEAAT16032024518500.PDS', 'bbox': [0, 0, 0, 0], 'geometry': {"coordinates": [0, 0], "type": "Point"},
                            'properties': {'datetime': '1970-01-01T00:27:28.050499079Z',
                                           'start_datetime': '2016-01-31T18:00:00.009057Z',
                                           'end_datetime': '2016-01-31T19:59:59.991043Z',
                                           'created': '2016-02-01T02:45:59.639000Z'},
                            'collection': 'ATMS_SCIENCE_Group___001', 'links': [{"href": ".", "rel": "collection"}], 'assets': {'data': {
                'href': 's3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS',
                'title': 'P1570515ATMSSCIENCEAAT16032024518500.PDS',
                'description': 'P1570515ATMSSCIENCEAAT16032024518500.PDS'}, 'metadata__data': {
                'href': 's3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518501.PDS',
                'title': 'P1570515ATMSSCIENCEAAT16032024518501.PDS',
                'description': 'P1570515ATMSSCIENCEAAT16032024518501.PDS'}, 'metadata__xml': {
                'href': 's3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518501.PDS.xml',
                'title': 'P1570515ATMSSCIENCEAAT16032024518501.PDS.xml',
                'description': 'P1570515ATMSSCIENCEAAT16032024518501.PDS.xml'}, 'metadata__cmr': {
                'href': 's3://am-uds-dev-cumulus-internal/ATMS_SCIENCE_Group___1/P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml',
                'title': 'P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml',
                'description': 'P1570515ATMSSCIENCEAAT16032024518500.PDS.cmr.xml'}}}
        self.assertEqual(json.dumps(sample_stac_item, sort_keys=True), json.dumps(stac_item, sort_keys=True), 'wrong stac item')
        stac_validator = JsonValidator(json.loads(STAC_ITEM_SCHEMA))
        # self.assertEqual(None, stac_validator.validate(raw), f'invalid stac format: {stac_validator}')
        self.assertEqual(None, stac_validator.validate(stac_item), f'invalid stac format: {stac_validator}')
        return
