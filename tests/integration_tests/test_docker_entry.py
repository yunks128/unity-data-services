import logging
import math

logging.basicConfig(level=20, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")

import json
import os
import tempfile
from glob import glob
from sys import argv
from unittest import TestCase

from pystac import Item, Asset, Catalog, Link, ItemCollection

from cumulus_lambda_functions.lib.constants import Constants

from cumulus_lambda_functions.docker_entrypoint.__main__ import choose_process
from cumulus_lambda_functions.lib.time_utils import TimeUtils
from cumulus_lambda_functions.lib.utils.file_utils import FileUtils


class TestDockerEntry(TestCase):
    def test_01_search_part_01(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'L0_SNPP_ATMS_SCIENCE___1'
        os.environ['LIMITS'] = '4000'
        os.environ['DATE_FROM'] = '1990-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2022-01-14T11:59:59Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            search_result_str = choose_process()
            search_result = json.loads(search_result_str)
            self.assertTrue('type' in search_result, f'missing type in search_result')
            item_collections = ItemCollection.from_dict(search_result)
            # self.assertTrue(isinstance(search_result, list), f'search_result is not list: {search_result}')
            self.assertEqual(len(item_collections.items), 4000, f'wrong length')
            search_result = set([k.id for k in item_collections.items])
            self.assertEqual(len(search_result), 4000, f'wrong length. not unique')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
            self.assertEqual(sorted(json.dumps(FileUtils.read_json(os.environ['OUTPUT_FILE']))), sorted(search_result_str), f'not identical result')
        return

    def test_01_search_part_02(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'SNDR_SNPP_ATMS_L1A___1'
        os.environ['LIMITS'] = '100'
        os.environ['DATE_FROM'] = '2016-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2016-01-14T11:59:59Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        print(search_result)
        search_result = json.loads(search_result)
        self.assertTrue('type' in search_result, f'missing type in search_result')
        item_collections = ItemCollection.from_dict(search_result)
        # self.assertTrue(isinstance(search_result, list), f'search_result is not list: {search_result}')
        self.assertEqual(len(item_collections.items), 20, f'wrong length')
        search_result = set([k.id for k in item_collections.items])
        self.assertEqual(len(search_result),20, f'wrong length. not unique')
        return

    def test_01_search_part_03(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'L0_SNPP_ATMS_SCIENCE___1'
        os.environ['LIMITS'] = '-1'
        os.environ['DATE_FROM'] = '1990-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2022-01-14T11:59:59Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        search_result = json.loads(search_result)
        self.assertTrue('type' in search_result, f'missing type in search_result')
        item_collections = ItemCollection.from_dict(search_result)
        # self.assertTrue(isinstance(search_result, list), f'search_result is not list: {search_result}')
        self.assertEqual(len(item_collections.items), 4381, f'wrong length')
        search_result = set([k.id for k in item_collections.items])
        self.assertEqual(len(search_result), 4381, f'wrong length. not unique')
        return

    def test_01_search_part_04(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'L0_SNPP_ATMS_SCIENCE___1'
        os.environ['LIMITS'] = '347'
        os.environ['DATE_FROM'] = '1990-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2022-01-14T11:59:59Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        search_result = json.loads(search_result)
        self.assertTrue('type' in search_result, f'missing type in search_result')
        item_collections = ItemCollection.from_dict(search_result)
        self.assertEqual(len(item_collections.items), 347, f'wrong length')
        search_result = set([k.id for k in item_collections.items])
        self.assertEqual(len(search_result), 347, f'wrong length. not unique')
        return

    def test_01_search_part_05(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'L0_SNPP_ATMS_SCIENCE___1'
        os.environ['LIMITS'] = '37'
        os.environ['DATE_FROM'] = '1990-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2022-01-14T11:59:59Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        search_result = json.loads(search_result)
        self.assertTrue('type' in search_result, f'missing type in search_result')
        item_collections = ItemCollection.from_dict(search_result)
        self.assertEqual(len(item_collections.items), 37, f'wrong length')
        return

    def test_01_1_search_cmr_part_01(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'C1996881146-POCLOUD'  # 'C1666605425-PODAAC'  # C1996881146-POCLOUD
        os.environ['LIMITS'] = '2120'
        os.environ['DATE_FROM'] = '2002-06-01T12:06:00.000Z'
        os.environ['DATE_TO'] = '2011-10-04T06:51:45.000Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'CMR'
        os.environ['CMR_BASE_URL'] = 'https://cmr.earthdata.nasa.gov'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        search_result = json.loads(search_result)
        self.assertTrue('type' in search_result, f'missing type in search_result')
        item_collections = ItemCollection.from_dict(search_result)
        self.assertEqual(len(item_collections.items), 2120, f'wrong length')
        return

    def test_01_1_search_cmr_part_02(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'C1996881146-POCLOUD'  # 'C1666605425-PODAAC'  # C1996881146-POCLOUD
        os.environ['LIMITS'] = '23'
        os.environ['DATE_FROM'] = '2002-06-01T12:06:00.000Z'
        os.environ['DATE_TO'] = '2011-10-04T06:51:45.000Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'CMR'
        os.environ['CMR_BASE_URL'] = 'https://cmr.earthdata.nasa.gov'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        search_result = json.loads(search_result)
        self.assertTrue('type' in search_result, f'missing type in search_result')
        item_collections = ItemCollection.from_dict(search_result)
        self.assertEqual(len(item_collections.items), 23, f'wrong length')
        return

    def test_02_download(self):
        granule_json = '{"numberMatched": 20, "numberReturned": 20, "stac_version": "1.0.0", "type": "FeatureCollection", "links": [{"rel": "self", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=0"}, {"rel": "root", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com"}, {"rel": "next", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=100"}, {"rel": "prev", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=0"}], "features": [{"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.01", "properties": {"start_datetime": "2016-01-14T09:54:00Z", "end_datetime": "2016-01-14T10:00:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:39.830000Z", "datetime": "2022-08-15T06:26:37.029000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.08", "properties": {"start_datetime": "2016-01-14T10:36:00Z", "end_datetime": "2016-01-14T10:42:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.078000Z", "datetime": "2022-08-15T06:26:19.333000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.06", "properties": {"start_datetime": "2016-01-14T10:24:00Z", "end_datetime": "2016-01-14T10:30:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.068000Z", "datetime": "2022-08-15T06:26:18.641000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.18", "properties": {"start_datetime": "2016-01-14T11:36:00Z", "end_datetime": "2016-01-14T11:42:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.060000Z", "datetime": "2022-08-15T06:26:19.698000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.04", "properties": {"start_datetime": "2016-01-14T10:12:00Z", "end_datetime": "2016-01-14T10:18:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.050000Z", "datetime": "2022-08-15T06:26:19.491000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.16", "properties": {"start_datetime": "2016-01-14T11:24:00Z", "end_datetime": "2016-01-14T11:30:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.917000Z", "datetime": "2022-08-15T06:26:19.027000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.17", "properties": {"start_datetime": "2016-01-14T11:30:00Z", "end_datetime": "2016-01-14T11:36:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.907000Z", "datetime": "2022-08-15T06:26:19.042000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.10", "properties": {"start_datetime": "2016-01-14T10:48:00Z", "end_datetime": "2016-01-14T10:54:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.446000Z", "datetime": "2022-08-15T06:26:18.730000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.14", "properties": {"start_datetime": "2016-01-14T11:12:00Z", "end_datetime": "2016-01-14T11:18:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.354000Z", "datetime": "2022-08-15T06:26:17.758000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.12", "properties": {"start_datetime": "2016-01-14T11:00:00Z", "end_datetime": "2016-01-14T11:06:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.344000Z", "datetime": "2022-08-15T06:26:17.938000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.09", "properties": {"start_datetime": "2016-01-14T10:42:00Z", "end_datetime": "2016-01-14T10:48:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:24.910000Z", "datetime": "2022-08-15T06:26:20.688000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.20", "properties": {"start_datetime": "2016-01-14T11:48:00Z", "end_datetime": "2016-01-14T11:54:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.929000Z", "datetime": "2022-08-15T06:26:19.091000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.15", "properties": {"start_datetime": "2016-01-14T11:18:00Z", "end_datetime": "2016-01-14T11:24:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.732000Z", "datetime": "2022-08-15T06:26:19.282000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.07", "properties": {"start_datetime": "2016-01-14T10:30:00Z", "end_datetime": "2016-01-14T10:36:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.371000Z", "datetime": "2022-08-15T06:26:19.047000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.19", "properties": {"start_datetime": "2016-01-14T11:42:00Z", "end_datetime": "2016-01-14T11:48:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.268000Z", "datetime": "2022-08-15T06:26:18.576000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.03", "properties": {"start_datetime": "2016-01-14T10:06:00Z", "end_datetime": "2016-01-14T10:12:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.930000Z", "datetime": "2022-08-15T06:26:17.714000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.11", "properties": {"start_datetime": "2016-01-14T10:54:00Z", "end_datetime": "2016-01-14T11:00:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.863000Z", "datetime": "2022-08-15T06:26:17.648000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.05", "properties": {"start_datetime": "2016-01-14T10:18:00Z", "end_datetime": "2016-01-14T10:24:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.649000Z", "datetime": "2022-08-15T06:26:18.060000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.13", "properties": {"start_datetime": "2016-01-14T11:06:00Z", "end_datetime": "2016-01-14T11:12:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.277000Z", "datetime": "2022-08-15T06:26:18.090000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.02", "properties": {"start_datetime": "2016-01-14T10:00:00Z", "end_datetime": "2016-01-14T10:06:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.169000Z", "datetime": "2022-08-15T06:26:17.466000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}]}'
        granule_json = json.loads(granule_json)
        os.environ['STAC_JSON'] = json.dumps(granule_json)
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'S3'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            os.environ['DOWNLOAD_DIR'] = tmp_dir_name
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            self.assertEqual(len(download_result['features']) + 2, len(glob(os.path.join(tmp_dir_name, '*'))), f'downloaded file does not match')
            error_file = os.path.join(tmp_dir_name, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            download_result = download_result['features']
            self.assertTrue('assets' in download_result[0], f'no assets in download_result: {download_result}')
            downloaded_file_hrefs = set([k['assets']['data']['href'] for k in download_result])
            for each_granule in zip(granule_json['features'], download_result):
                remote_filename = os.path.basename(each_granule[0]['assets']['data']['href'])
                self.assertTrue(os.path.join('.', remote_filename) in downloaded_file_hrefs, f'mismatched: {remote_filename}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__daac(self):
        granule_json = '{"type": "FeatureCollection", "stac_version": "1.0.0", "numberMatched": 3413, "numberReturned": 23, "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac?collection_concept_id=C1996881146-POCLOUD&page_size=23&temporal%5B%5D=2002-06-01T12%3A06%3A00.000Z%2C2011-10-04T06%3A51%3A45.000Z&page_num=1"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "next", "body": {"collection_concept_id": "C1996881146-POCLOUD", "page_num": "2", "page_size": "23", "temporal": ["2002-06-01T12:06:00.000Z,2011-10-04T06:51:45.000Z"], "temporal[]": "2002-06-01T12:06:00.000Z,2011-10-04T06:51:45.000Z"}, "method": "POST", "merge": true, "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac"}], "context": {"returned": 23, "limit": 1000000, "matched": 3413}, "features": [{"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-01T21:00:00.000Z", "start_datetime": "2002-06-01T21:00:00.000Z", "end_datetime": "2002-06-02T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020602090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020602090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020602090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106835-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-02T21:00:00.000Z", "start_datetime": "2002-06-02T21:00:00.000Z", "end_datetime": "2002-06-03T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020603090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020603090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020603090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106890-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-03T21:00:00.000Z", "start_datetime": "2002-06-03T21:00:00.000Z", "end_datetime": "2002-06-04T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020604090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020604090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020604090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106962-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-04T21:00:00.000Z", "start_datetime": "2002-06-04T21:00:00.000Z", "end_datetime": "2002-06-05T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020605090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020605090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020605090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106862-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.umm_json"}]}]}'
        granule_json = json.loads(granule_json)
        os.environ[Constants.EDL_USERNAME] = '/unity/uds/user/wphyo/edl_username'
        os.environ[Constants.EDL_PASSWORD] = '/unity/uds/user/wphyo/edl_dwssap'
        os.environ[Constants.EDL_PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.EDL_BASE_URL] = 'urs.earthdata.nasa.gov'
        os.environ['STAC_JSON'] = json.dumps(granule_json)
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'DAAC'
        os.environ['DOWNLOADING_KEYS'] = 'metadata'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            os.environ['DOWNLOAD_DIR'] = tmp_dir_name
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            self.assertEqual(len(download_result['features']) + 2, len(glob(os.path.join(tmp_dir_name, '*'))), f'downloaded file does not match')
            error_file = os.path.join(tmp_dir_name, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            download_result = download_result['features']
            self.assertTrue('assets' in download_result[0], f'no assets in download_result: {download_result}')
            downloaded_file_hrefs = set([k['assets']['metadata']['href'] for k in download_result])
            for each_granule in zip(granule_json['features'], download_result):
                remote_filename = os.path.basename(each_granule[0]['assets']['metadata']['href'])
                self.assertTrue(os.path.join('.', remote_filename) in downloaded_file_hrefs, f'mismatched: {remote_filename}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__daac__from_file(self):
        granule_json = '{"type": "FeatureCollection", "stac_version": "1.0.0", "numberMatched": 3413, "numberReturned": 23, "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac?collection_concept_id=C1996881146-POCLOUD&page_size=23&temporal%5B%5D=2002-06-01T12%3A06%3A00.000Z%2C2011-10-04T06%3A51%3A45.000Z&page_num=1"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "next", "body": {"collection_concept_id": "C1996881146-POCLOUD", "page_num": "2", "page_size": "23", "temporal": ["2002-06-01T12:06:00.000Z,2011-10-04T06:51:45.000Z"], "temporal[]": "2002-06-01T12:06:00.000Z,2011-10-04T06:51:45.000Z"}, "method": "POST", "merge": true, "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac"}], "context": {"returned": 23, "limit": 1000000, "matched": 3413}, "features": [{"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020601090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-01T21:00:00.000Z", "start_datetime": "2002-06-01T21:00:00.000Z", "end_datetime": "2002-06-02T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020602090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020602090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020602090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106835-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106835-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-02T21:00:00.000Z", "start_datetime": "2002-06-02T21:00:00.000Z", "end_datetime": "2002-06-03T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020603090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020603090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020603090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106890-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106890-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-03T21:00:00.000Z", "start_datetime": "2002-06-03T21:00:00.000Z", "end_datetime": "2002-06-04T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020604090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020604090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020604090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106962-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106962-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-06-04T21:00:00.000Z", "start_datetime": "2002-06-04T21:00:00.000Z", "end_datetime": "2002-06-05T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"metadata": {"href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.xml", "type": "application/xml"}, "opendap": {"title": "OPeNDAP request URL", "href": "https://opendap.earthdata.nasa.gov/providers/POCLOUD/collections/GHRSST%20Level%204%20MUR%20Global%20Foundation%20Sea%20Surface%20Temperature%20Analysis%20(v4.1)/granules/20020605090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1"}, "data": {"title": "Download 20020605090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc", "href": "https://archive.podaac.earthdata.nasa.gov/podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1/20020605090000-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2028106862-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2028106862-POCLOUD.umm_json"}]}]}'
        granule_json = json.loads(granule_json)
        os.environ[Constants.EDL_USERNAME] = '/unity/uds/user/wphyo/edl_username'
        os.environ[Constants.EDL_PASSWORD] = '/unity/uds/user/wphyo/edl_dwssap'
        os.environ[Constants.EDL_PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.EDL_BASE_URL] = 'urs.earthdata.nasa.gov'
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'DAAC'
        os.environ['DOWNLOADING_KEYS'] = 'metadata'

        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            granule_json_file = os.path.join(tmp_dir_name, 'input_file.json')
            downloading_dir = os.path.join(tmp_dir_name, 'downloading_dir')
            FileUtils.mk_dir_p(downloading_dir)
            FileUtils.write_json(granule_json_file, granule_json)
            os.environ['STAC_JSON'] = granule_json_file
            os.environ['DOWNLOAD_DIR'] = downloading_dir
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            self.assertEqual(len(download_result['features']) + 1, len(glob(os.path.join(downloading_dir, '*'))), f'downloaded file does not match')
            error_file = os.path.join(downloading_dir, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            download_result = download_result['features']
            self.assertTrue('assets' in download_result[0], f'no assets in download_result: {download_result}')
            downloaded_file_hrefs = set([k['assets']['metadata']['href'] for k in download_result])
            for each_granule in zip(granule_json['features'], download_result):
                remote_filename = os.path.basename(each_granule[0]['assets']['metadata']['href'])
                self.assertTrue(os.path.join('.', remote_filename) in downloaded_file_hrefs, f'mismatched: {remote_filename}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__daac_502__from_file(self):
        granule_json = '''{
  "type": "FeatureCollection",
  "stac_version": "1.0.0",
  "numberMatched": 242,
  "numberReturned": 10,
  "links": [
    {
      "rel": "self",
      "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac?collection_concept_id=C2011289787-GES_DISC&page_size=10&temporal%5B%5D=2021-02-01T00%3A00%3A00Z%2C2021-02-02T00%3A00%3A00Z&page_num=1"
    },
    {
      "rel": "root",
      "href": "https://cmr.earthdata.nasa.gov:443/search/"
    },
    {
      "rel": "next",
      "body": {
        "collection_concept_id": "C2011289787-GES_DISC",
        "page_num": "2",
        "page_size": "10",
        "temporal": [
          "2021-02-01T00:00:00Z,2021-02-02T00:00:00Z"
        ],
        "temporal[]": "2021-02-01T00:00:00Z,2021-02-02T00:00:00Z"
      },
      "method": "POST",
      "merge": true,
      "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac"
    }
  ],
  "context": {
    "returned": 10,
    "limit": 1000000,
    "matched": 242
  },
  "features": [
    {
      "properties": {
        "datetime": "2021-01-31T23:54:00.000Z",
        "start_datetime": "2021-01-31T23:54:00.000Z",
        "end_datetime": "2021-02-01T00:00:00.000Z"
      },
      "bbox": [
        -164.22,
        -59.19,
        -127.42,
        -34.4
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327589-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/031/SNDR.SS1330.CHIRP.20210131T2354.m06.g240.L1_J1.std.v02_48.G.200408101645.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210131T2354.m06.g240.L1_J1.std.v02_48.G.200408101645.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/031/SNDR.SS1330.CHIRP.20210131T2354.m06.g240.L1_J1.std.v02_48.G.200408101645.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -127.42,
              -53.49
            ],
            [
              -140.59,
              -34.4
            ],
            [
              -164.22,
              -38.4
            ],
            [
              -161.84,
              -59.19
            ],
            [
              -127.42,
              -53.49
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327589-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327589-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327589-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327589-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:00:00.000Z",
        "start_datetime": "2021-02-01T00:00:00.000Z",
        "end_datetime": "2021-02-01T00:06:00.000Z"
      },
      "bbox": [
        -167.61,
        -38.28,
        -140.62,
        -14.06
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327600-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0000.m06.g001.L1_J1.std.v02_48.G.200408101657.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0000.m06.g001.L1_J1.std.v02_48.G.200408101657.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0000.m06.g001.L1_J1.std.v02_48.G.200408101657.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -140.62,
              -34.28
            ],
            [
              -147.98,
              -14.06
            ],
            [
              -167.61,
              -17.44
            ],
            [
              -164.31,
              -38.28
            ],
            [
              -140.62,
              -34.28
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327600-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327600-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327600-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327600-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:06:00.000Z",
        "start_datetime": "2021-02-01T00:06:00.000Z",
        "end_datetime": "2021-02-01T00:12:00.000Z"
      },
      "bbox": [
        -171.93,
        -17.31,
        -148.0,
        6.74
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327624-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0006.m06.g002.L1_J1.std.v02_48.G.200408101655.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0006.m06.g002.L1_J1.std.v02_48.G.200408101655.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0006.m06.g002.L1_J1.std.v02_48.G.200408101655.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -148.0,
              -13.93
            ],
            [
              -153.06,
              6.74
            ],
            [
              -171.93,
              3.48
            ],
            [
              -167.7,
              -17.31
            ],
            [
              -148.0,
              -13.93
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327624-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327624-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327624-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327624-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:12:00.000Z",
        "start_datetime": "2021-02-01T00:12:00.000Z",
        "end_datetime": "2021-02-01T00:18:00.000Z"
      },
      "bbox": [
        -177.83,
        3.6,
        -153.07,
        27.71
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327611-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0012.m06.g003.L1_J1.std.v02_48.G.200408101705.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0012.m06.g003.L1_J1.std.v02_48.G.200408101705.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0012.m06.g003.L1_J1.std.v02_48.G.200408101705.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -153.07,
              6.87
            ],
            [
              -156.97,
              27.71
            ],
            [
              -177.83,
              24.15
            ],
            [
              -172.02,
              3.6
            ],
            [
              -153.07,
              6.87
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327611-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327611-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327611-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327611-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:18:00.000Z",
        "start_datetime": "2021-02-01T00:18:00.000Z",
        "end_datetime": "2021-02-01T00:24:00.000Z"
      },
      "bbox": [
        172.75,
        24.27,
        -156.97,
        48.71
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327811-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0018.m06.g004.L1_J1.std.v02_48.G.200408101716.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0018.m06.g004.L1_J1.std.v02_48.G.200408101716.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0018.m06.g004.L1_J1.std.v02_48.G.200408101716.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -156.97,
              27.84
            ],
            [
              -160.07,
              48.71
            ],
            [
              172.75,
              44.19
            ],
            [
              -177.94,
              24.27
            ],
            [
              -156.97,
              27.84
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327811-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327811-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327811-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327811-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:24:00.000Z",
        "start_datetime": "2021-02-01T00:24:00.000Z",
        "end_datetime": "2021-02-01T00:30:00.000Z"
      },
      "bbox": [
        152.97,
        44.29,
        -160.05,
        69.67
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327763-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0024.m06.g005.L1_J1.std.v02_48.G.200408101749.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0024.m06.g005.L1_J1.std.v02_48.G.200408101749.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0024.m06.g005.L1_J1.std.v02_48.G.200408101749.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -160.05,
              48.84
            ],
            [
              -161.92,
              69.67
            ],
            [
              152.97,
              62.28
            ],
            [
              172.6,
              44.29
            ],
            [
              -160.05,
              48.84
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327763-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327763-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327763-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327763-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:30:00.000Z",
        "start_datetime": "2021-02-01T00:30:00.000Z",
        "end_datetime": "2021-02-01T00:36:00.000Z"
      },
      "bbox": [
        -180.0,
        62.35,
        180.0,
        90.0
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327782-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0030.m06.g006.L1_J1.std.v02_48.G.200408101747.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0030.m06.g006.L1_J1.std.v02_48.G.200408101747.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0030.m06.g006.L1_J1.std.v02_48.G.200408101747.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -161.83,
              69.79
            ],
            [
              -45.22,
              88.83
            ],
            [
              101.33,
              71.69
            ],
            [
              152.69,
              62.35
            ],
            [
              -161.83,
              69.79
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327782-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327782-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327782-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327782-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:36:00.000Z",
        "start_datetime": "2021-02-01T00:36:00.000Z",
        "end_datetime": "2021-02-01T00:42:00.000Z"
      },
      "bbox": [
        -41.63,
        61.83,
        100.91,
        89.26695841076085
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327806-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0036.m06.g007.L1_J1.std.v02_48.G.200408101747.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0036.m06.g007.L1_J1.std.v02_48.G.200408101747.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0036.m06.g007.L1_J1.std.v02_48.G.200408101747.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -41.63,
              88.73
            ],
            [
              6.42,
              68.41
            ],
            [
              50.76,
              61.83
            ],
            [
              100.91,
              71.65
            ],
            [
              -41.63,
              88.73
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327806-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327806-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327806-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327806-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:42:00.000Z",
        "start_datetime": "2021-02-01T00:42:00.000Z",
        "end_datetime": "2021-02-01T00:48:00.000Z"
      },
      "bbox": [
        4.45,
        43.65,
        50.62,
        68.30202151196218
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327773-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0042.m06.g008.L1_J1.std.v02_48.G.200408101807.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0042.m06.g008.L1_J1.std.v02_48.G.200408101807.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0042.m06.g008.L1_J1.std.v02_48.G.200408101807.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              6.29,
              68.29
            ],
            [
              4.45,
              47.46
            ],
            [
              31.46,
              43.65
            ],
            [
              50.62,
              61.71
            ],
            [
              6.29,
              68.29
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327773-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327773-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327773-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327773-GES_DISC.umm_json"
        }
      ]
    },
    {
      "properties": {
        "datetime": "2021-02-01T00:48:00.000Z",
        "start_datetime": "2021-02-01T00:48:00.000Z",
        "end_datetime": "2021-02-01T00:54:00.000Z"
      },
      "bbox": [
        1.29,
        23.6,
        31.41,
        47.33
      ],
      "assets": {
        "metadata": {
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327751-GES_DISC.xml",
          "type": "application/xml"
        },
        "opendap": {
          "title": "The OPENDAP location for the granule. (GET DATA : OPENDAP DATA)",
          "href": "https://sounder.gesdisc.eosdis.nasa.gov/opendap/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0048.m06.g009.L1_J1.std.v02_48.G.200408101736.nc",
          "type": "application/x-netcdf"
        },
        "data": {
          "title": "Download SNDR.SS1330.CHIRP.20210201T0048.m06.g009.L1_J1.std.v02_48.G.200408101736.nc",
          "href": "https://data.gesdisc.earthdata.nasa.gov/data/CHIRP/SNDR13CHRP1.2/2021/032/SNDR.SS1330.CHIRP.20210201T0048.m06.g009.L1_J1.std.v02_48.G.200408101736.nc"
        }
      },
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [
              4.35,
              47.33
            ],
            [
              1.29,
              26.46
            ],
            [
              22.18,
              23.6
            ],
            [
              31.41,
              43.52
            ],
            [
              4.35,
              47.33
            ]
          ]
        ]
      },
      "stac_extensions": [],
      "id": "G2031327751-GES_DISC",
      "stac_version": "1.0.0",
      "collection": "C2011289787-GES_DISC",
      "links": [
        {
          "rel": "self",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327751-GES_DISC.stac"
        },
        {
          "rel": "parent",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "collection",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C2011289787-GES_DISC.stac"
        },
        {
          "rel": "root",
          "href": "https://cmr.earthdata.nasa.gov:443/search/"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327751-GES_DISC.json"
        },
        {
          "rel": "via",
          "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2031327751-GES_DISC.umm_json"
        }
      ]
    }
  ]
}'''
        granule_json = json.loads(granule_json)
        os.environ[Constants.EDL_USERNAME] = '/unity/uds/user/wphyo/edl_username'
        os.environ[Constants.EDL_PASSWORD] = '/unity/uds/user/wphyo/edl_dwssap'
        os.environ[Constants.EDL_PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.EDL_BASE_URL] = 'urs.earthdata.nasa.gov'
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'DAAC'
        os.environ['DOWNLOADING_KEYS'] = 'data'

        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            granule_json_file = os.path.join(tmp_dir_name, 'input_file.json')
            downloading_dir = os.path.join(tmp_dir_name, 'downloading_dir')
            FileUtils.mk_dir_p(downloading_dir)
            FileUtils.write_json(granule_json_file, granule_json)
            os.environ['STAC_JSON'] = granule_json_file
            os.environ['DOWNLOAD_DIR'] = downloading_dir
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            error_file = os.path.join(downloading_dir, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            self.assertEqual(len(download_result['features']) + 1, len(glob(os.path.join(downloading_dir, '*'))), f'downloaded file does not match')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__daac_error(self):  # TODO update this later
        granule_json = '{"type": "FeatureCollection", "stac_version": "1.0.0", "numberMatched": 3413, "numberReturned": 23, "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac?collection_concept_id=C1996881146-POCLOUD&page_size=23&temporal%5B%5D=2002-06-01T12%3A06%3A00.000Z%2C2011-10-04T06%3A51%3A45.000Z&page_num=1"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "next", "body": {"collection_concept_id": "C1996881146-POCLOUD", "page_num": "2", "page_size": "23", "temporal": ["2002-06-01T12:06:00.000Z,2011-10-04T06:51:45.000Z"], "temporal[]": "2002-06-01T12:06:00.000Z,2011-10-04T06:51:45.000Z"}, "method": "POST", "merge": true, "href": "https://cmr.earthdata.nasa.gov:443/search/granules.stac"}], "context": {"returned": 23, "limit": 1000000, "matched": 3413}, "features": [{"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601161248-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00414.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601172624-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00415.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601190536-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00416.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601204344-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00417.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601222152-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00418.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602000000-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00419.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602013912-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00420.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602031720-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00421.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602045528-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00422.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602063440-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00423.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602081248-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00424.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602095056-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00425.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602112904-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00426.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602130816-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00427.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602144624-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00428.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602162432-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00429.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602180240-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00430.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602194152-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00431.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602212000-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00432.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602225808-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00433.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/154/20020603003616-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00434.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/154/20020603021528-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00435.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}, {"properties": {"datetime": "2002-05-31T21:00:00.000Z", "start_datetime": "2002-05-31T21:00:00.000Z", "end_datetime": "2002-06-01T21:00:00.000Z"}, "bbox": [-180.0, -90.0, 180.0, 90.0], "assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/154/20020603035336-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00436.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}, "type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[-180.0, -90.0], [180.0, -90.0], [180.0, 90.0], [-180.0, 90.0], [-180.0, -90.0]]]}, "stac_extensions": [], "id": "G2030963432-POCLOUD", "stac_version": "1.0.0", "collection": "C1996881146-POCLOUD", "links": [{"rel": "self", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.stac"}, {"rel": "parent", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "collection", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/C1996881146-POCLOUD.stac"}, {"rel": "root", "href": "https://cmr.earthdata.nasa.gov:443/search/"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.json"}, {"rel": "via", "href": "https://cmr.earthdata.nasa.gov:443/search/concepts/G2030963432-POCLOUD.umm_json"}]}]}'
        # granule_json = [{"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601161248-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00414.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601172624-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00415.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601190536-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00416.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601204344-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00417.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/152/20020601222152-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00418.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602000000-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00419.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602013912-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00420.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602031720-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00421.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602045528-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00422.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602063440-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00423.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602081248-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00424.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602095056-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00425.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602112904-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00426.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602130816-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00427.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602144624-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00428.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602162432-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00429.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602180240-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00430.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602194152-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00431.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602212000-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00432.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/153/20020602225808-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00433.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/154/20020603003616-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00434.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/154/20020603021528-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00435.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}, {"assets": {"data": {"href": "https://podaac-tools.jpl.nasa.gov/drive/files/allData/ghrsst/data/GDS2/L2P/AMSRE/REMSS/v7/2002/154/20020603035336-REMSS-L2P_GHRSST-SSTsubskin-AMSRE-l2b_v07a_r00436.dat-v02.0-fv01.0.nc", "title": "The HTTP location for the granule."}}}]
        granule_json = json.loads(granule_json)
        os.environ[Constants.EDL_USERNAME] = '/unity/uds/user/wphyo/edl_username'
        os.environ[Constants.EDL_PASSWORD] = '/unity/uds/user/wphyo/edl_dwssap'
        os.environ[Constants.EDL_PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.EDL_BASE_URL] = 'urs.earthdata.nasa.gov'
        os.environ['STAC_JSON'] = json.dumps(granule_json)
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'DAAC'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            # TODO this is downloading a login page HTML
            os.environ['DOWNLOAD_DIR'] = tmp_dir_name
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            print(glob(os.path.join(tmp_dir_name, '*')))
            self.assertEqual(len(download_result['features']) + 2, len(glob(os.path.join(tmp_dir_name, '*'))), f'downloaded file does not match')
            error_file = os.path.join(tmp_dir_name, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__from_file(self):
        granule_json = '{"numberMatched": 20, "numberReturned": 20, "stac_version": "1.0.0", "type": "FeatureCollection", "links": [{"rel": "self", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=0"}, {"rel": "root", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com"}, {"rel": "next", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=100"}, {"rel": "prev", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=0"}], "features": [{"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.01", "properties": {"start_datetime": "2016-01-14T09:54:00Z", "end_datetime": "2016-01-14T10:00:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:39.830000Z", "datetime": "2022-08-15T06:26:37.029000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.08", "properties": {"start_datetime": "2016-01-14T10:36:00Z", "end_datetime": "2016-01-14T10:42:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.078000Z", "datetime": "2022-08-15T06:26:19.333000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.06", "properties": {"start_datetime": "2016-01-14T10:24:00Z", "end_datetime": "2016-01-14T10:30:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.068000Z", "datetime": "2022-08-15T06:26:18.641000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.18", "properties": {"start_datetime": "2016-01-14T11:36:00Z", "end_datetime": "2016-01-14T11:42:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.060000Z", "datetime": "2022-08-15T06:26:19.698000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.04", "properties": {"start_datetime": "2016-01-14T10:12:00Z", "end_datetime": "2016-01-14T10:18:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.050000Z", "datetime": "2022-08-15T06:26:19.491000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.16", "properties": {"start_datetime": "2016-01-14T11:24:00Z", "end_datetime": "2016-01-14T11:30:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.917000Z", "datetime": "2022-08-15T06:26:19.027000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.17", "properties": {"start_datetime": "2016-01-14T11:30:00Z", "end_datetime": "2016-01-14T11:36:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.907000Z", "datetime": "2022-08-15T06:26:19.042000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.10", "properties": {"start_datetime": "2016-01-14T10:48:00Z", "end_datetime": "2016-01-14T10:54:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.446000Z", "datetime": "2022-08-15T06:26:18.730000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.14", "properties": {"start_datetime": "2016-01-14T11:12:00Z", "end_datetime": "2016-01-14T11:18:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.354000Z", "datetime": "2022-08-15T06:26:17.758000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.12", "properties": {"start_datetime": "2016-01-14T11:00:00Z", "end_datetime": "2016-01-14T11:06:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:25.344000Z", "datetime": "2022-08-15T06:26:17.938000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.09", "properties": {"start_datetime": "2016-01-14T10:42:00Z", "end_datetime": "2016-01-14T10:48:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:24.910000Z", "datetime": "2022-08-15T06:26:20.688000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.20", "properties": {"start_datetime": "2016-01-14T11:48:00Z", "end_datetime": "2016-01-14T11:54:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.929000Z", "datetime": "2022-08-15T06:26:19.091000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.15", "properties": {"start_datetime": "2016-01-14T11:18:00Z", "end_datetime": "2016-01-14T11:24:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.732000Z", "datetime": "2022-08-15T06:26:19.282000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.07", "properties": {"start_datetime": "2016-01-14T10:30:00Z", "end_datetime": "2016-01-14T10:36:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.371000Z", "datetime": "2022-08-15T06:26:19.047000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.19", "properties": {"start_datetime": "2016-01-14T11:42:00Z", "end_datetime": "2016-01-14T11:48:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:23.268000Z", "datetime": "2022-08-15T06:26:18.576000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.03", "properties": {"start_datetime": "2016-01-14T10:06:00Z", "end_datetime": "2016-01-14T10:12:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.930000Z", "datetime": "2022-08-15T06:26:17.714000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.11", "properties": {"start_datetime": "2016-01-14T10:54:00Z", "end_datetime": "2016-01-14T11:00:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.863000Z", "datetime": "2022-08-15T06:26:17.648000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.05", "properties": {"start_datetime": "2016-01-14T10:18:00Z", "end_datetime": "2016-01-14T10:24:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.649000Z", "datetime": "2022-08-15T06:26:18.060000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.13", "properties": {"start_datetime": "2016-01-14T11:06:00Z", "end_datetime": "2016-01-14T11:12:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.277000Z", "datetime": "2022-08-15T06:26:18.090000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.02", "properties": {"start_datetime": "2016-01-14T10:00:00Z", "end_datetime": "2016-01-14T10:06:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:22.169000Z", "datetime": "2022-08-15T06:26:17.466000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc", "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}]}'
        granule_json = json.loads(granule_json)
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'S3'
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            granule_json_file = os.path.join(tmp_dir_name, 'input_file.json')
            downloading_dir = os.path.join(tmp_dir_name, 'downloading_dir')
            FileUtils.mk_dir_p(downloading_dir)
            FileUtils.write_json(granule_json_file, granule_json)
            os.environ['STAC_JSON'] = granule_json_file
            os.environ['DOWNLOAD_DIR'] = downloading_dir
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            self.assertEqual(len(download_result['features']) + 1, len(glob(os.path.join(downloading_dir, '*'))), f'downloaded file does not match')
            error_file = os.path.join(downloading_dir, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            download_result = download_result['features']
            self.assertTrue('assets' in download_result[0], f'no assets in download_result: {download_result}')
            downloaded_file_hrefs = set([k['assets']['data']['href'] for k in download_result])
            for each_granule in zip(granule_json['features'], download_result):
                remote_filename = os.path.basename(each_granule[0]['assets']['data']['href'])
                self.assertTrue(os.path.join('.', remote_filename) in downloaded_file_hrefs, f'mismatched: {remote_filename}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__from_file_large(self):
        granule_json = FileUtils.read_json('./stage-in.json')
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        os.environ[Constants.EDL_USERNAME] = '/unity/uds/user/wphyo/edl_username'
        os.environ[Constants.EDL_PASSWORD] = '/unity/uds/user/wphyo/edl_dwssap'
        os.environ[Constants.EDL_PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.EDL_BASE_URL] = 'urs.earthdata.nasa.gov'
        os.environ['STAC_JSON'] = json.dumps(granule_json)
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'DAAC'
        os.environ['PARALLEL_COUNT'] = '5'

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            print(tmp_dir_name)
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            granule_json_file = os.path.join(tmp_dir_name, 'input_file.json')
            downloading_dir = os.path.join(tmp_dir_name, 'downloading_dir')
            FileUtils.mk_dir_p(downloading_dir)
            FileUtils.write_json(granule_json_file, granule_json)
            os.environ['STAC_JSON'] = granule_json_file
            os.environ['DOWNLOAD_DIR'] = downloading_dir
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            print(len(download_result['features']))
            self.assertTrue('features' in download_result, f'missing features in download_result')
            self.assertEqual(len(download_result['features']) + 1, len(glob(os.path.join(downloading_dir, '*'))), f'downloaded file does not match')
            error_file = os.path.join(downloading_dir, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            download_result = download_result['features']
            self.assertTrue('assets' in download_result[0], f'no assets in download_result: {download_result}')
            downloaded_file_hrefs = set([k['assets']['data']['href'] for k in download_result])
            for each_granule in zip(granule_json['features'], download_result):
                remote_filename = os.path.basename(each_granule[0]['assets']['data']['href'])
                self.assertTrue(os.path.join('.', remote_filename) in downloaded_file_hrefs, f'mismatched: {remote_filename}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_02_download__from_http(self):
        granule_json = '{"numberMatched": 20, "numberReturned": 20, "stac_version": "1.0.0", "type": "FeatureCollection", "links": [{"rel": "self", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=0"}, {"rel": "root", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com"}, {"rel": "next", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=100"}, {"rel": "prev", "href": "https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test/am-uds-dapa/collections/SNDR_SNPP_ATMS_L1A___1/items?datetime=2016-01-14T08:00:00Z/2016-01-14T11:59:59Z&limit=100&offset=0"}], "features": [{"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.01", "properties": {"start_datetime": "2016-01-14T09:54:00Z", "end_datetime": "2016-01-14T10:00:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:39.830000Z", "datetime": "2022-08-15T06:26:37.029000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "https://raw.githubusercontent.com/unity-sds/unity-data-services/develop/README.md", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.08", "properties": {"start_datetime": "2016-01-14T10:36:00Z", "end_datetime": "2016-01-14T10:42:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.078000Z", "datetime": "2022-08-15T06:26:19.333000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "https://raw.githubusercontent.com/unity-sds/unity-data-services/develop/CHANGELOG.md", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.06", "properties": {"start_datetime": "2016-01-14T10:24:00Z", "end_datetime": "2016-01-14T10:30:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.068000Z", "datetime": "2022-08-15T06:26:18.641000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "https://raw.githubusercontent.com/unity-sds/unity-data-services/develop/CODE_OF_CONDUCT.md", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.18", "properties": {"start_datetime": "2016-01-14T11:36:00Z", "end_datetime": "2016-01-14T11:42:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.060000Z", "datetime": "2022-08-15T06:26:19.698000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "https://raw.githubusercontent.com/unity-sds/unity-data-services/develop/CONTRIBUTING.md", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}, {"type": "Feature", "stac_version": "1.0.0", "id": "SNDR.SNPP.ATMS.L1A.nominal2.04", "properties": {"start_datetime": "2016-01-14T10:12:00Z", "end_datetime": "2016-01-14T10:18:00Z", "created": "2020-12-14T13:50:00Z", "updated": "2022-08-15T06:26:26.050000Z", "datetime": "2022-08-15T06:26:19.491000Z"}, "geometry": {"type": "Point", "coordinates": [0.0, 0.0]}, "links": [{"rel": "collection", "href": "."}], "assets": {"data": {"href": "https://raw.githubusercontent.com/unity-sds/unity-data-services/develop/LICENSE", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc"}, "metadata__data": {"href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas"}, "metadata__cmr": {"href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml", "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml", "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml"}}, "bbox": [0.0, 0.0, 0.0, 0.0], "stac_extensions": [], "collection": "SNDR_SNPP_ATMS_L1A___1"}]}'
        granule_json = json.loads(granule_json)
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        os.environ['GRANULES_DOWNLOAD_TYPE'] = 'HTTP'
        os.environ['PARALLEL_COUNT'] = '3'
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            granule_json_file = os.path.join(tmp_dir_name, 'input_file.json')
            downloading_dir = os.path.join(tmp_dir_name, 'downloading_dir')
            FileUtils.mk_dir_p(downloading_dir)
            FileUtils.write_json(granule_json_file, granule_json)
            os.environ['STAC_JSON'] = granule_json_file
            os.environ['DOWNLOAD_DIR'] = downloading_dir
            download_result_str = choose_process()
            download_result = json.loads(download_result_str)
            self.assertTrue('features' in download_result, f'missing features in download_result')
            self.assertEqual(len(download_result['features']) + 1, len(glob(os.path.join(downloading_dir, '*'))), f'downloaded file does not match')
            error_file = os.path.join(tmp_dir_name, 'error.log')
            if FileUtils.file_exist(error_file):
                self.assertTrue(False, f'some downloads failed. error.log exists. {FileUtils.read_json(error_file)}')
            download_result = download_result['features']
            self.assertTrue('assets' in download_result[0], f'no assets in download_result: {download_result}')
            downloaded_file_hrefs = set([k['assets']['data']['href'] for k in download_result])
            for each_granule in zip(granule_json['features'], download_result):
                remote_filename = os.path.basename(each_granule[0]['assets']['data']['href'])
                self.assertTrue(os.path.join('.', remote_filename) in downloaded_file_hrefs, f'mismatched: {remote_filename}')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def not_in_used_test_03_upload(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['VERIFY_SSL'] = 'FALSE'

        os.environ['COLLECTION_ID'] = 'NEW_COLLECTION_EXAMPLE_L1B___9'
        os.environ['STAGING_BUCKET'] = 'uds-test-cumulus-staging'

        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        os.environ['GRANULES_UPLOAD_TYPE'] = 'S3'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('UPLOAD')

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            os.environ['UPLOAD_DIR'] = tmp_dir_name
            with open(os.path.join(tmp_dir_name, 'test_file01.nc'), 'w') as ff:
                ff.write('sample_file')
            with open(os.path.join(tmp_dir_name, 'test_file01.nc.cas'), 'w') as ff:
                ff.write('''<?xml version="1.0" encoding="UTF-8" ?>
        <cas:metadata xmlns:cas="http://oodt.jpl.nasa.gov/1.0/cas">
            <keyval type="scalar">
                <key>AggregateDir</key>
                <val>snppatmsl1a</val>
            </keyval>
            <keyval type="vector">
                <key>AutomaticQualityFlag</key>
                <val>Passed</val>
            </keyval>
            <keyval type="vector">
                <key>BuildId</key>
                <val>v01.43.00</val>
            </keyval>
            <keyval type="vector">
                <key>CollectionLabel</key>
                <val>L1AMw_nominal2</val>
            </keyval>
            <keyval type="scalar">
                <key>DataGroup</key>
                <val>sndr</val>
            </keyval>
            <keyval type="scalar">
                <key>EndDateTime</key>
                <val>2016-01-14T10:06:00.000Z</val>
            </keyval>
            <keyval type="scalar">
                <key>EndTAI93</key>
                <val>726919569.000</val>
            </keyval>
            <keyval type="scalar">
                <key>FileFormat</key>
                <val>nc4</val>
            </keyval>
            <keyval type="scalar">
                <key>FileLocation</key>
                <val>/pge/out</val>
            </keyval>
            <keyval type="scalar">
                <key>Filename</key>
                <val>SNDR.SNPP.ATMS.L1A.nominal2.02.nc</val>
            </keyval>
            <keyval type="vector">
                <key>GranuleNumber</key>
                <val>101</val>
            </keyval>
            <keyval type="scalar">
                <key>JobId</key>
                <val>f163835c-9945-472f-bee2-2bc12673569f</val>
            </keyval>
            <keyval type="scalar">
                <key>ModelId</key>
                <val>urn:npp:SnppAtmsL1a</val>
            </keyval>
            <keyval type="scalar">
                <key>NominalDate</key>
                <val>2016-01-14</val>
            </keyval>
            <keyval type="vector">
                <key>ProductName</key>
                <val>SNDR.SNPP.ATMS.20160114T1000.m06.g101.L1A.L1AMw_nominal2.v03_15_00.D.201214135000.nc</val>
            </keyval>
            <keyval type="scalar">
                <key>ProductType</key>
                <val>SNDR_SNPP_ATMS_L1A</val>
            </keyval>
            <keyval type="scalar">
                <key>ProductionDateTime</key>
                <val>2020-12-14T13:50:00.000Z</val>
            </keyval>
            <keyval type="vector">
                <key>ProductionLocation</key>
                <val>Sounder SIPS: JPL/Caltech (Dev)</val>
            </keyval>
            <keyval type="vector">
                <key>ProductionLocationCode</key>
                <val>D</val>
            </keyval>
            <keyval type="scalar">
                <key>RequestId</key>
                <val>1215</val>
            </keyval>
            <keyval type="scalar">
                <key>StartDateTime</key>
                <val>2016-01-14T10:00:00.000Z</val>
            </keyval>
            <keyval type="scalar">
                <key>StartTAI93</key>
                <val>726919209.000</val>
            </keyval>
            <keyval type="scalar">
                <key>TaskId</key>
                <val>8c3ae101-8f7c-46c8-b5c6-63e7b6d3c8cd</val>
            </keyval>
        </cas:metadata>''')
            upload_result = choose_process()
            print(upload_result)
            self.assertEqual(1, len(upload_result), 'wrong length of upload_result features')
            upload_result = upload_result[0]
            self.assertTrue('assets' in upload_result, 'missing assets')
            self.assertTrue('metadata' in upload_result['assets'], 'missing assets#metadata')
            self.assertTrue('href' in upload_result['assets']['metadata'], 'missing assets#metadata#href')
            self.assertTrue(
                upload_result['assets']['metadata']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/'))
            self.assertTrue('data' in upload_result['assets'], 'missing assets#data')
            self.assertTrue('href' in upload_result['assets']['data'], 'missing assets#data#href')
            self.assertTrue(upload_result['assets']['data']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/'))
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def not_in_used_test_03_upload_catalog(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['VERIFY_SSL'] = 'FALSE'

        os.environ['COLLECTION_ID'] = 'NEW_COLLECTION_EXAMPLE_L1B___9'
        os.environ['STAGING_BUCKET'] = 'uds-test-cumulus-staging'

        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        os.environ['GRANULES_UPLOAD_TYPE'] = 'CATALOG_S3'

        if len(argv) > 1:
            argv.pop(-1)
        argv.append('UPLOAD')

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            os.environ['UPLOAD_DIR'] = ''  # not needed
            os.environ['CATALOG_FILE'] = os.path.join(tmp_dir_name, 'catalog.json')
            with open(os.path.join(tmp_dir_name, 'test_file01.nc'), 'w') as ff:
                ff.write('sample_file')
            with open(os.path.join(tmp_dir_name, 'test_file01.nc.cas'), 'w') as ff:
                ff.write('''<?xml version="1.0" encoding="UTF-8" ?>
        <cas:metadata xmlns:cas="http://oodt.jpl.nasa.gov/1.0/cas">
            <keyval type="scalar">
                <key>AggregateDir</key>
                <val>snppatmsl1a</val>
            </keyval>
            <keyval type="vector">
                <key>AutomaticQualityFlag</key>
                <val>Passed</val>
            </keyval>
            <keyval type="vector">
                <key>BuildId</key>
                <val>v01.43.00</val>
            </keyval>
            <keyval type="vector">
                <key>CollectionLabel</key>
                <val>L1AMw_nominal2</val>
            </keyval>
            <keyval type="scalar">
                <key>DataGroup</key>
                <val>sndr</val>
            </keyval>
            <keyval type="scalar">
                <key>EndDateTime</key>
                <val>2016-01-14T10:06:00.000Z</val>
            </keyval>
            <keyval type="scalar">
                <key>EndTAI93</key>
                <val>726919569.000</val>
            </keyval>
            <keyval type="scalar">
                <key>FileFormat</key>
                <val>nc4</val>
            </keyval>
            <keyval type="scalar">
                <key>FileLocation</key>
                <val>/pge/out</val>
            </keyval>
            <keyval type="scalar">
                <key>Filename</key>
                <val>SNDR.SNPP.ATMS.L1A.nominal2.02.nc</val>
            </keyval>
            <keyval type="vector">
                <key>GranuleNumber</key>
                <val>101</val>
            </keyval>
            <keyval type="scalar">
                <key>JobId</key>
                <val>f163835c-9945-472f-bee2-2bc12673569f</val>
            </keyval>
            <keyval type="scalar">
                <key>ModelId</key>
                <val>urn:npp:SnppAtmsL1a</val>
            </keyval>
            <keyval type="scalar">
                <key>NominalDate</key>
                <val>2016-01-14</val>
            </keyval>
            <keyval type="vector">
                <key>ProductName</key>
                <val>SNDR.SNPP.ATMS.20160114T1000.m06.g101.L1A.L1AMw_nominal2.v03_15_00.D.201214135000.nc</val>
            </keyval>
            <keyval type="scalar">
                <key>ProductType</key>
                <val>SNDR_SNPP_ATMS_L1A</val>
            </keyval>
            <keyval type="scalar">
                <key>ProductionDateTime</key>
                <val>2020-12-14T13:50:00.000Z</val>
            </keyval>
            <keyval type="vector">
                <key>ProductionLocation</key>
                <val>Sounder SIPS: JPL/Caltech (Dev)</val>
            </keyval>
            <keyval type="vector">
                <key>ProductionLocationCode</key>
                <val>D</val>
            </keyval>
            <keyval type="scalar">
                <key>RequestId</key>
                <val>1215</val>
            </keyval>
            <keyval type="scalar">
                <key>StartDateTime</key>
                <val>2016-01-14T10:00:00.000Z</val>
            </keyval>
            <keyval type="scalar">
                <key>StartTAI93</key>
                <val>726919209.000</val>
            </keyval>
            <keyval type="scalar">
                <key>TaskId</key>
                <val>8c3ae101-8f7c-46c8-b5c6-63e7b6d3c8cd</val>
            </keyval>
        </cas:metadata>''')
            stac_item = Item(id='NA',
                             geometry={
                                "type": "Point",
                                "coordinates": [0.0, 0.0]
                             },
                             bbox=[0.0, 0.0, 0.0, 0.0],
                             datetime=TimeUtils().parse_from_unix(0, True).get_datetime_obj(),
                             properties={
                                 "start_datetime": "2016-01-31T18:00:00.009057Z",
                                 "end_datetime": "2016-01-31T19:59:59.991043Z",
                                 "created": "2016-02-01T02:45:59.639000Z",
                                 "updated": "2022-03-23T15:48:21.578000Z",
                                 "datetime": "2022-03-23T15:48:19.079000Z"
                             },
                             collection='NA',
                             assets={
                                'data': Asset(os.path.join(tmp_dir_name, 'test_file01.nc'), title='main data'),
                                'metadata__cas': Asset(os.path.join(tmp_dir_name, 'test_file01.nc.cas'), title='metadata cas'),
                                'metadata__stac': Asset(os.path.join(tmp_dir_name, 'test_file01.nc.stac.json'), title='metadata stac'),
                             })
            with open(os.path.join(tmp_dir_name, 'test_file01.nc.stac.json'), 'w') as ff:
                ff.write(json.dumps(stac_item.to_dict(False, False)))
            catalog = Catalog(
                id='NA',
                description='NA')
            catalog.set_self_href(os.environ['CATALOG_FILE'])
            catalog.add_link(Link('child', os.path.join(tmp_dir_name, 'test_file01.nc.stac.json'), 'application/json'))
            with open(os.environ['CATALOG_FILE'], 'w') as ff:
                ff.write(json.dumps(catalog.to_dict(False, False)))

            upload_result = choose_process()
            print(upload_result)
            self.assertEqual(1, len(upload_result), 'wrong length of upload_result features')
            upload_result = upload_result[0]
            self.assertTrue('assets' in upload_result, 'missing assets')
            self.assertTrue('metadata__cas' in upload_result['assets'], 'missing assets#metadata__cas')
            self.assertTrue('href' in upload_result['assets']['metadata__cas'], 'missing assets#metadata__cas#href')
            self.assertTrue(
                upload_result['assets']['metadata__cas']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/'))
            self.assertTrue('data' in upload_result['assets'], 'missing assets#data')
            self.assertTrue('href' in upload_result['assets']['data'], 'missing assets#data#href')
            self.assertTrue(upload_result['assets']['data']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/'))
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_03_upload_complete_catalog(self):
        os.environ['VERIFY_SSL'] = 'FALSE'

        os.environ['COLLECTION_ID'] = 'NEW_COLLECTION_EXAMPLE_L1B___9'
        os.environ['STAGING_BUCKET'] = 'uds-test-cumulus-staging'

        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        # os.environ['GRANULES_UPLOAD_TYPE'] = 'UPLOAD_S3_BY_STAC_CATALOG'
        # defaulted to this value

        if len(argv) > 1:
            argv.pop(-1)
        argv.append('UPLOAD')

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            os.environ['UPLOAD_DIR'] = ''  # not needed
            os.environ['CATALOG_FILE'] = os.path.join(tmp_dir_name, 'catalog.json')
            granules_dir = os.path.join(tmp_dir_name, 'some_granules')
            FileUtils.mk_dir_p(granules_dir)
            catalog = Catalog(
                id='NA',
                description='NA')
            catalog.set_self_href(os.environ['CATALOG_FILE'])

            total_files = 10
            for i in range(1, total_files+1):
                filename = f'test_file{i:02d}'
                with open(os.path.join(granules_dir, f'{filename}.nc'), 'w') as ff:
                    ff.write('sample_file')
                with open(os.path.join(granules_dir, f'{filename}.nc.cas'), 'w') as ff:
                    ff.write('''<?xml version="1.0" encoding="UTF-8" ?>
            <cas:metadata xmlns:cas="http://oodt.jpl.nasa.gov/1.0/cas">
                <keyval type="scalar">
                    <key>AggregateDir</key>
                    <val>snppatmsl1a</val>
                </keyval>
                <keyval type="vector">
                    <key>AutomaticQualityFlag</key>
                    <val>Passed</val>
                </keyval>
                <keyval type="vector">
                    <key>BuildId</key>
                    <val>v01.43.00</val>
                </keyval>
                <keyval type="vector">
                    <key>CollectionLabel</key>
                    <val>L1AMw_nominal2</val>
                </keyval>
                <keyval type="scalar">
                    <key>DataGroup</key>
                    <val>sndr</val>
                </keyval>
                <keyval type="scalar">
                    <key>EndDateTime</key>
                    <val>2016-01-14T10:06:00.000Z</val>
                </keyval>
                <keyval type="scalar">
                    <key>EndTAI93</key>
                    <val>726919569.000</val>
                </keyval>
                <keyval type="scalar">
                    <key>FileFormat</key>
                    <val>nc4</val>
                </keyval>
                <keyval type="scalar">
                    <key>FileLocation</key>
                    <val>/pge/out</val>
                </keyval>
                <keyval type="scalar">
                    <key>Filename</key>
                    <val>SNDR.SNPP.ATMS.L1A.nominal2.02.nc</val>
                </keyval>
                <keyval type="vector">
                    <key>GranuleNumber</key>
                    <val>101</val>
                </keyval>
                <keyval type="scalar">
                    <key>JobId</key>
                    <val>f163835c-9945-472f-bee2-2bc12673569f</val>
                </keyval>
                <keyval type="scalar">
                    <key>ModelId</key>
                    <val>urn:npp:SnppAtmsL1a</val>
                </keyval>
                <keyval type="scalar">
                    <key>NominalDate</key>
                    <val>2016-01-14</val>
                </keyval>
                <keyval type="vector">
                    <key>ProductName</key>
                    <val>SNDR.SNPP.ATMS.20160114T1000.m06.g101.L1A.L1AMw_nominal2.v03_15_00.D.201214135000.nc</val>
                </keyval>
                <keyval type="scalar">
                    <key>ProductType</key>
                    <val>SNDR_SNPP_ATMS_L1A</val>
                </keyval>
                <keyval type="scalar">
                    <key>ProductionDateTime</key>
                    <val>2020-12-14T13:50:00.000Z</val>
                </keyval>
                <keyval type="vector">
                    <key>ProductionLocation</key>
                    <val>Sounder SIPS: JPL/Caltech (Dev)</val>
                </keyval>
                <keyval type="vector">
                    <key>ProductionLocationCode</key>
                    <val>D</val>
                </keyval>
                <keyval type="scalar">
                    <key>RequestId</key>
                    <val>1215</val>
                </keyval>
                <keyval type="scalar">
                    <key>StartDateTime</key>
                    <val>2016-01-14T10:00:00.000Z</val>
                </keyval>
                <keyval type="scalar">
                    <key>StartTAI93</key>
                    <val>726919209.000</val>
                </keyval>
                <keyval type="scalar">
                    <key>TaskId</key>
                    <val>8c3ae101-8f7c-46c8-b5c6-63e7b6d3c8cd</val>
                </keyval>
            </cas:metadata>''')
                stac_item = Item(id=filename,
                                 geometry={
                                    "type": "Point",
                                    "coordinates": [0.0, 0.0]
                                 },
                                 bbox=[0.0, 0.0, 0.0, 0.0],
                                 datetime=TimeUtils().parse_from_unix(0, True).get_datetime_obj(),
                                 properties={
                                     "start_datetime": "2016-01-31T18:00:00.009057Z",
                                     "end_datetime": "2016-01-31T19:59:59.991043Z",
                                     "created": "2016-02-01T02:45:59.639000Z",
                                     "updated": "2022-03-23T15:48:21.578000Z",
                                     "datetime": "2022-03-23T15:48:19.079000Z"
                                 },
                                 href=os.path.join('some_granules', f'{filename}.nc.stac.json'),
                                 collection='NA',
                                 assets={
                                    'data': Asset(os.path.join('.', 'test_file01.nc'), title='main data'),
                                    'metadata__cas': Asset(os.path.join('.', 'test_file01.nc.cas'), title='metadata cas'),
                                    'metadata__stac': Asset(os.path.join('.', 'test_file01.nc.stac.json'), title='metadata stac'),
                                 })
                with open(os.path.join(granules_dir, f'{filename}.nc.stac.json'), 'w') as ff:
                    ff.write(json.dumps(stac_item.to_dict(False, False)))
                catalog.add_link(Link('item', os.path.join('some_granules', f'{filename}.nc.stac.json'), 'application/json'))
            print(json.dumps(catalog.to_dict(False, False)))
            with open(os.environ['CATALOG_FILE'], 'w') as ff:
                ff.write(json.dumps(catalog.to_dict(False, False)))

            upload_result_str = choose_process()
            upload_result = json.loads(upload_result_str)
            print(upload_result)
            self.assertTrue('features' in upload_result, 'missing features')
            self.assertEqual(total_files, len(upload_result['features']), 'wrong length of upload_result features')
            upload_result = upload_result['features'][0]
            self.assertTrue('assets' in upload_result, 'missing assets')
            self.assertTrue('metadata__cas' in upload_result['assets'], 'missing assets#metadata__cas')
            self.assertTrue('href' in upload_result['assets']['metadata__cas'], 'missing assets#metadata__cas#href')
            self.assertTrue(upload_result['assets']['metadata__cas']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/{os.environ["COLLECTION_ID"]}/'))
            self.assertTrue('data' in upload_result['assets'], 'missing assets#data')
            self.assertTrue('href' in upload_result['assets']['data'], 'missing assets#data#href')
            self.assertTrue(upload_result['assets']['data']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/{os.environ["COLLECTION_ID"]}/'))
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
            """
            Example output: 
            {
                'type': 'FeatureCollection', 
                'features': [{
                    'type': 'Feature', 
                    'stac_version': '1.0.0', 
                    'id': 'NEW_COLLECTION_EXAMPLE_L1B___9:test_file01',
                    'properties': {'start_datetime': '2016-01-31T18:00:00.009057Z',
                                'end_datetime': '2016-01-31T19:59:59.991043Z', 'created': '2016-02-01T02:45:59.639000Z',
                                'updated': '2022-03-23T15:48:21.578000Z', 'datetime': '1970-01-01T00:00:00Z'},
                    'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [], 
                    'assets': {'data': {
                        'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc',
                        'title': 'main data'}, 'metadata__cas': {
                        'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.cas',
                        'title': 'metadata cas'}, 'metadata__stac': {
                        'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.stac.json',
                        'title': 'metadata stac'}}, 
                    'bbox': [0.0, 0.0, 0.0, 0.0], 
                    'stac_extensions': [],
                    'collection': 'NEW_COLLECTION_EXAMPLE_L1B___9'}]}
            """
        return

    def test_04_catalog(self):
        upload_result = [{'id': 'NEW_COLLECTION_EXAMPLE_L1B___9:test_file01', 'collection': 'NEW_COLLECTION_EXAMPLE_L1B___9', 'assets': {'metadata': {'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.cas'}, 'data': {'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc'}}}]
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['PROVIDER_ID'] = 'SNPP'
        os.environ['GRANULES_CATALOG_TYPE'] = 'UNITY'
        os.environ['UPLOADED_FILES_JSON'] = json.dumps(upload_result)
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('CATALOG')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            catalog_result_str = choose_process()
            catalog_result = json.loads(catalog_result_str)
            self.assertEqual('registered', catalog_result, 'wrong status')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
        return

    def test_04_catalog_from_file(self):
        upload_result = [{'id': 'NEW_COLLECTION_EXAMPLE_L1B___9:test_file01', 'collection': 'NEW_COLLECTION_EXAMPLE_L1B___9', 'assets': {'metadata': {'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.cas'}, 'data': {'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc'}}}]
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['PROVIDER_ID'] = 'SNPP'
        os.environ['GRANULES_CATALOG_TYPE'] = 'UNITY'
        os.environ['DELAY_SECOND'] = '35'
        os.environ['REPEAT_TIMES'] = '3'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('CATALOG')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            input_file_path = os.path.join(tmp_dir_name, 'uploaded_files.json')
            FileUtils.write_json(input_file_path, upload_result)
            os.environ['UPLOADED_FILES_JSON'] = input_file_path
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            catalog_result_str = choose_process()
            catalog_result = json.loads(catalog_result_str)
            self.assertTrue('cataloging_request_status' in catalog_result, f'missing cataloging_request_status')
            self.assertTrue('status_result' in catalog_result, f'missing status_result')
            self.assertEqual(catalog_result['cataloging_request_status'], 'registered', f'mismatched cataloging_request_status value')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')

            status_result = catalog_result['status_result']

            self.assertTrue('cataloged' in status_result, f'missing cataloged')
            self.assertTrue('missing_granules' in status_result, f'missing missing_granules')
            self.assertTrue('registered_granules' in status_result, f'missing registered_granules')
            self.assertTrue(isinstance(status_result['cataloged'], bool), f'cataloged is not boolean: {status_result["cataloged"]}')
            # Example result: {'cataloging_request_status': 'registered', 'status_result': {'cataloged': False, 'missing_granules': ['NEW_COLLECTION_EXAMPLE_L1B___9:test_file01'], 'registered_granules': []}}
        return

    def test_04_catalog_from_file_item_collection(self):
        upload_result = {'type': 'FeatureCollection', 'features': [
            {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'NEW_COLLECTION_EXAMPLE_L1B___9:test_file01',
             'properties': {'start_datetime': '2016-01-31T18:00:00.009057Z', 'end_datetime': '2016-01-31T19:59:59.991043Z',
                            'created': '2016-02-01T02:45:59.639000Z', 'updated': '2022-03-23T15:48:21.578000Z',
                            'datetime': '1970-01-01T00:00:00Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]},
             'links': [], 'assets': {
                'data': {'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc',
                         'title': 'main data'}, 'metadata__cas': {
                    'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.cas',
                    'title': 'metadata cas'}, 'metadata__stac': {
                    'href': 's3://uds-test-cumulus-staging/NEW_COLLECTION_EXAMPLE_L1B___9/NEW_COLLECTION_EXAMPLE_L1B___9:test_file01/test_file01.nc.stac.json',
                    'title': 'metadata stac'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [],
             'collection': 'NEW_COLLECTION_EXAMPLE_L1B___9'}]}
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['PROVIDER_ID'] = 'SNPP'
        os.environ['GRANULES_CATALOG_TYPE'] = 'UNITY'
        # os.environ['DELAY_SECOND'] = '5'
        # os.environ['REPEAT_TIMES'] = '3'
        os.environ['DELAY_SECOND'] = '35'
        os.environ['REPEAT_TIMES'] = '3'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('CATALOG')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            input_file_path = os.path.join(tmp_dir_name, 'uploaded_files.json')
            FileUtils.write_json(input_file_path, upload_result)
            os.environ['UPLOADED_FILES_JSON'] = input_file_path
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            catalog_result_str = choose_process()
            catalog_result = json.loads(catalog_result_str)
            self.assertTrue('cataloging_request_status' in catalog_result, f'missing cataloging_request_status')
            self.assertTrue('status_result' in catalog_result, f'missing status_result')
            self.assertEqual(catalog_result['cataloging_request_status'], 'registered', f'mismatched cataloging_request_status value')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')
            # TODO update this after it is deployed to MCP Test
            status_result = catalog_result['status_result']

            self.assertTrue('cataloged' in status_result, f'missing cataloged')
            self.assertTrue('missing_granules' in status_result, f'missing missing_granules')
            self.assertTrue('registered_granules' in status_result, f'missing registered_granules')
            self.assertTrue(isinstance(status_result['cataloged'], bool), f'cataloged is not boolean: {status_result["cataloged"]}')
        return

    def test_04_catalog_from_file_item_collection_large(self):
        upload_result = FileUtils.read_json('./stage-out-results.json')
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ[Constants.CLIENT_ID] = '71g0c73jl77gsqhtlfg2ht388c'  # MCP Dev
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['PROVIDER_ID'] = 'SNPP'
        os.environ['GRANULES_CATALOG_TYPE'] = 'UNITY'
        os.environ['CHUNK_SIZE'] = '250'
        # os.environ['DELAY_SECOND'] = '5'
        # os.environ['REPEAT_TIMES'] = '3'

        if len(argv) > 1:
            argv.pop(-1)
        argv.append('CATALOG')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            input_file_path = os.path.join(tmp_dir_name, 'uploaded_files.json')
            FileUtils.write_json(input_file_path, upload_result)
            os.environ['UPLOADED_FILES_JSON'] = input_file_path
            os.environ['OUTPUT_FILE'] = os.path.join(tmp_dir_name, 'some_output', 'output.json')
            catalog_result_str = choose_process()
            catalog_result = json.loads(catalog_result_str)
            self.assertTrue(isinstance(catalog_result, list), f'catalog_result is not list. {catalog_result}')
            self.assertEqual(len(catalog_result), math.ceil(len(upload_result['features']) / 250), f'mismatched catalog_result count')

            catalog_result = catalog_result[0]
            self.assertTrue('cataloging_request_status' in catalog_result, f'missing cataloging_request_status')
            self.assertTrue('status_result' in catalog_result, f'missing status_result')
            self.assertEqual(catalog_result['cataloging_request_status'], {'message': 'processing'}, f'mismatched cataloging_request_status value')
            self.assertTrue(FileUtils.file_exist(os.environ['OUTPUT_FILE']), f'missing output file')

            status_result = catalog_result['status_result']
            # TODO disabling this as we are not waiting for them to be registered.
            # self.assertTrue('cataloged' in status_result, f'missing cataloged')
            # self.assertTrue('missing_granules' in status_result, f'missing missing_granules')
            # self.assertTrue('registered_granules' in status_result, f'missing registered_granules')
            # self.assertTrue(isinstance(status_result['cataloged'], bool), f'cataloged is not boolean: {status_result["cataloged"]}')
        return
