import os
import tempfile
from unittest import TestCase

from cumulus_lambda_functions.cumulus_stac.granules_catalog import GranulesCatalog
from cumulus_lambda_functions.lib.utils.file_utils import FileUtils


class TestGranulesCatalog(TestCase):
    def test_get_child_link_hrefs(self):
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "sample-id",
                "description": "Reference: https://github.com/radiantearth/stac-spec/blob/master/examples/catalog.json",
                "links": [
                    {
                        "href": "/absolute/path/to/stac/granules/json/file",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    },
                    {
                        "href": "/absolute/path/to/stac/granules/json/file2",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    }
                ]
            }
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, granules_catalog)
            pystac_catalog = GranulesCatalog()
            hrefs = pystac_catalog.get_child_link_hrefs(granules_catalog_path)
            self.assertEqual(hrefs, ['/absolute/path/to/stac/granules/json/file', '/absolute/path/to/stac/granules/json/file2'])
        return

    def test_get_granules_item(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
            "data": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc"
            },
            "metadata__data": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas"
            },
            "metadata__cmr": {
              "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml"
            }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            pystac_catalog = GranulesCatalog().get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
        return

    def test_extract_assets_href(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
            "data": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc"
            },
            "metadata__data": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas"
            },
            "metadata__cmr": {
              "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml"
            }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {'data': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc', 'metadata__data': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas', 'metadata__cmr': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}
            self.assertEqual(assets, expected_assets, 'wrong assets')
        return

    def test_update_assets_href(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
            "data": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc"
            },
            "metadata__data": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas"
            },
            "metadata__cmr": {
              "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml"
            }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {'data': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc', 'metadata__data': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas', 'metadata__cmr': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}
            self.assertEqual(assets, expected_assets, 'wrong assets')
            updating_assets = {
                'data': 'file:///absolute/file/some/file/data',
                'metadata__data': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                'metadata__extra': '/absolute/file/some/file/metadata__extra',
                'metadata__cmr': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'
            }
            gc.update_assets_href(pystac_catalog, updating_assets)
            updated_assets = gc.extract_assets_href(pystac_catalog)
            self.assertEqual(updated_assets, updating_assets, 'wrong updated assets')

        return
