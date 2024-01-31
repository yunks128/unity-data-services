import logging

logging.basicConfig(level=10, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")

import math
from unittest.mock import patch, MagicMock
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
        os.environ['COLLECTION_ID'] = 'URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2312041030'
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
        os.environ['API_PREFIX'] = 'sbx-uds-2-dapa'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev'
        os.environ['COLLECTION_ID'] = 'URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_COLLECTION___2312041030'
        os.environ['LIMITS'] = '37'
        os.environ['DATE_FROM'] = '1990-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2024-01-14T11:59:59Z'
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
            total_files = 10
            # os.environ['PARALLEL_COUNT'] = str(total_files)
            granules_dir = os.path.join(tmp_dir_name, 'some_granules')
            FileUtils.mk_dir_p(granules_dir)
            catalog = Catalog(
                id='NA',
                description='NA')
            catalog.set_self_href(os.environ['CATALOG_FILE'])

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

                                    'test_file01.nc': Asset(os.path.join('.', 'test_file01.nc'), title='test_file01.nc', roles=['data']),
                                    'test_file01.nc.cas': Asset(os.path.join('.', 'test_file01.nc.cas'), title='test_file01.nc.cas', roles=['metadata']),
                                    'test_file01.nc.stac.json': Asset(os.path.join('.', 'test_file01.nc.stac.json'), title='test_file01.nc.stac.json', roles=['metadata']),
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
            """
            {'type': 'Catalog', 'id': 'NA', 'stac_version': '1.0.0', 'description': 'NA', 'links': [{'rel': 'root', 'href': '/var/folders/33/xhq97d6s0dq78wg4h2smw23m0000gq/T/tmprew515jo/catalog.json', 'type': 'application/json'}, {'rel': 'item', 'href': '/var/folders/33/xhq97d6s0dq78wg4h2smw23m0000gq/T/tmprew515jo/successful_features.json', 'type': 'application/json'}, {'rel': 'item', 'href': '/var/folders/33/xhq97d6s0dq78wg4h2smw23m0000gq/T/tmprew515jo/failed_features.json', 'type': 'application/json'}]}
            """
            self.assertTrue('type' in upload_result, 'missing type')
            self.assertEqual(upload_result['type'], 'Catalog', 'missing type')
            upload_result = Catalog.from_dict(upload_result)
            child_links = [k.href for k in upload_result.get_links(rel='item')]
            self.assertEqual(len(child_links), 2, f'wrong length: {child_links}')
            self.assertTrue(FileUtils.file_exist(child_links[0]), f'missing file: {child_links[0]}')
            successful_feature_collection = ItemCollection.from_dict(FileUtils.read_json(child_links[0]))
            successful_feature_collection = list(successful_feature_collection.items)
            self.assertEqual(len(successful_feature_collection), total_files, f'wrong length: {successful_feature_collection}')

            self.assertTrue(FileUtils.file_exist(child_links[1]), f'missing file: {child_links[1]}')
            failed_feature_collection = ItemCollection.from_dict(FileUtils.read_json(child_links[1]))
            failed_feature_collection = list(failed_feature_collection.items)
            self.assertEqual(len(failed_feature_collection), 0, f'wrong length: {failed_feature_collection}')

            upload_result = successful_feature_collection[0].to_dict(False, False)
            print(f'example feature: {upload_result}')
            self.assertTrue('assets' in upload_result, 'missing assets')
            self.assertTrue('test_file01.nc.cas' in upload_result['assets'], 'missing assets#metadata asset: test_file01.nc.cas')
            self.assertTrue('href' in upload_result['assets']['test_file01.nc.cas'], 'missing assets#metadata__cas#href')
            self.assertTrue(upload_result['assets']['test_file01.nc.cas']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/{os.environ["COLLECTION_ID"]}/'))
            self.assertTrue('test_file01.nc' in upload_result['assets'], 'missing assets#data: test_file01.nc')
            self.assertTrue('href' in upload_result['assets']['test_file01.nc'], 'missing assets#data#href')
            self.assertTrue(upload_result['assets']['test_file01.nc']['href'].startswith(f's3://{os.environ["STAGING_BUCKET"]}/{os.environ["COLLECTION_ID"]}/'))
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

    def test_03_upload_complete_catalog_invalid_bucket(self):
        os.environ['VERIFY_SSL'] = 'FALSE'

        os.environ['COLLECTION_ID'] = 'NEW_COLLECTION_EXAMPLE_L1B___9'
        os.environ['STAGING_BUCKET'] = 'invalid_bucket'

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
            total_files = 10
            # os.environ['PARALLEL_COUNT'] = str(total_files)
            granules_dir = os.path.join(tmp_dir_name, 'some_granules')
            FileUtils.mk_dir_p(granules_dir)
            catalog = Catalog(
                id='NA',
                description='NA')
            catalog.set_self_href(os.environ['CATALOG_FILE'])

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
            """
            {'type': 'Catalog', 'id': 'NA', 'stac_version': '1.0.0', 'description': 'NA', 'links': [{'rel': 'root', 'href': '/var/folders/33/xhq97d6s0dq78wg4h2smw23m0000gq/T/tmprew515jo/catalog.json', 'type': 'application/json'}, {'rel': 'item', 'href': '/var/folders/33/xhq97d6s0dq78wg4h2smw23m0000gq/T/tmprew515jo/successful_features.json', 'type': 'application/json'}, {'rel': 'item', 'href': '/var/folders/33/xhq97d6s0dq78wg4h2smw23m0000gq/T/tmprew515jo/failed_features.json', 'type': 'application/json'}]}
            """
            self.assertTrue('type' in upload_result, 'missing type')
            self.assertEqual(upload_result['type'], 'Catalog', 'missing type')
            upload_result = Catalog.from_dict(upload_result)
            child_links = [k.href for k in upload_result.get_links(rel='item')]
            self.assertEqual(len(child_links), 2, f'wrong length: {child_links}')
            self.assertTrue(FileUtils.file_exist(child_links[0]), f'missing file: {child_links[0]}')
            successful_feature_collection = ItemCollection.from_dict(FileUtils.read_json(child_links[0]))
            successful_feature_collection = list(successful_feature_collection.items)
            self.assertEqual(len(successful_feature_collection), 0, f'wrong length: {successful_feature_collection}')

            self.assertTrue(FileUtils.file_exist(child_links[1]), f'missing file: {child_links[1]}')
            failed_feature_collection = ItemCollection.from_dict(FileUtils.read_json(child_links[1]))
            failed_feature_collection = list(failed_feature_collection.items)
            self.assertEqual(len(failed_feature_collection), total_files, f'wrong length: {failed_feature_collection}')

            upload_result = failed_feature_collection[0].to_dict(False, False)
            print(f'example feature: {upload_result}')
            self.assertTrue('properties' in upload_result, 'missing properties')
            self.assertTrue('upload_error' in upload_result['properties'], 'missing upload_error')
            self.assertTrue('An error occurred (NoSuchBucket)'in upload_result['properties']['upload_error'], f"wrong upload_error: {upload_result['properties']['upload_error']}")

            self.assertTrue('assets' in upload_result, 'missing assets')
            self.assertTrue('metadata__cas' in upload_result['assets'], 'missing assets#metadata__cas')
            self.assertTrue('href' in upload_result['assets']['metadata__cas'], 'missing assets#metadata__cas#href')
            self.assertTrue('data' in upload_result['assets'], 'missing assets#data')
            self.assertTrue('href' in upload_result['assets']['data'], 'missing assets#data#href')
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
