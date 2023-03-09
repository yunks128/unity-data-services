import json
import os
import tempfile
from glob import glob
from sys import argv
from unittest import TestCase

from cumulus_lambda_functions.lib.constants import Constants

from cumulus_lambda_functions.docker_entrypoint.__main__ import choose_process


class TestDockerEntry(TestCase):
    def test_01_search(self):
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
        search_result = json.loads(search_result)
        self.assertTrue(isinstance(search_result, list), f'search_result is not list: {search_result}')
        return

    def test_01_1_search_cmr(self):
        """
        :return:
        """
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'C1649553296-PODAAC'
        os.environ['LIMITS'] = '100'
        os.environ['DATE_FROM'] = '2016-01-14T08:00:00Z'
        os.environ['DATE_TO'] = '2016-01-14T11:59:59Z'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['GRANULES_SEARCH_DOMAIN'] = 'CMR'
        os.environ['CMR_BASE_URL'] = 'https://cmr.earthdata.nasa.gov'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('SEARCH')
        search_result = choose_process()
        search_result = json.loads(search_result)
        self.assertTrue(isinstance(search_result, list), f'search_result is not list: {search_result}')
        return

    def test_02_download(self):
        granule_json = [{'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.01', 'properties': {'start_datetime': '2016-01-14T09:54:00Z', 'end_datetime': '2016-01-14T10:00:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:39.830000Z', 'datetime': '2022-08-15T06:26:37.029000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.01.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.01.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.08', 'properties': {'start_datetime': '2016-01-14T10:36:00Z', 'end_datetime': '2016-01-14T10:42:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:26.078000Z', 'datetime': '2022-08-15T06:26:19.333000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.08.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.08.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.06', 'properties': {'start_datetime': '2016-01-14T10:24:00Z', 'end_datetime': '2016-01-14T10:30:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:26.068000Z', 'datetime': '2022-08-15T06:26:18.641000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.06.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.06.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.18', 'properties': {'start_datetime': '2016-01-14T11:36:00Z', 'end_datetime': '2016-01-14T11:42:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:26.060000Z', 'datetime': '2022-08-15T06:26:19.698000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.18.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.18.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.04', 'properties': {'start_datetime': '2016-01-14T10:12:00Z', 'end_datetime': '2016-01-14T10:18:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:26.050000Z', 'datetime': '2022-08-15T06:26:19.491000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.04.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.04.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.16', 'properties': {'start_datetime': '2016-01-14T11:24:00Z', 'end_datetime': '2016-01-14T11:30:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:25.917000Z', 'datetime': '2022-08-15T06:26:19.027000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.16.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.16.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.17', 'properties': {'start_datetime': '2016-01-14T11:30:00Z', 'end_datetime': '2016-01-14T11:36:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:25.907000Z', 'datetime': '2022-08-15T06:26:19.042000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.17.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.17.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.10', 'properties': {'start_datetime': '2016-01-14T10:48:00Z', 'end_datetime': '2016-01-14T10:54:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:25.446000Z', 'datetime': '2022-08-15T06:26:18.730000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.10.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.10.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.14', 'properties': {'start_datetime': '2016-01-14T11:12:00Z', 'end_datetime': '2016-01-14T11:18:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:25.354000Z', 'datetime': '2022-08-15T06:26:17.758000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.14.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.14.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.12', 'properties': {'start_datetime': '2016-01-14T11:00:00Z', 'end_datetime': '2016-01-14T11:06:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:25.344000Z', 'datetime': '2022-08-15T06:26:17.938000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.12.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.12.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.09', 'properties': {'start_datetime': '2016-01-14T10:42:00Z', 'end_datetime': '2016-01-14T10:48:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:24.910000Z', 'datetime': '2022-08-15T06:26:20.688000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.09.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.09.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.20', 'properties': {'start_datetime': '2016-01-14T11:48:00Z', 'end_datetime': '2016-01-14T11:54:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:23.929000Z', 'datetime': '2022-08-15T06:26:19.091000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.20.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.20.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.15', 'properties': {'start_datetime': '2016-01-14T11:18:00Z', 'end_datetime': '2016-01-14T11:24:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:23.732000Z', 'datetime': '2022-08-15T06:26:19.282000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.15.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.15.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.07', 'properties': {'start_datetime': '2016-01-14T10:30:00Z', 'end_datetime': '2016-01-14T10:36:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:23.371000Z', 'datetime': '2022-08-15T06:26:19.047000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.07.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.07.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.19', 'properties': {'start_datetime': '2016-01-14T11:42:00Z', 'end_datetime': '2016-01-14T11:48:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:23.268000Z', 'datetime': '2022-08-15T06:26:18.576000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.19.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.19.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.03', 'properties': {'start_datetime': '2016-01-14T10:06:00Z', 'end_datetime': '2016-01-14T10:12:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:22.930000Z', 'datetime': '2022-08-15T06:26:17.714000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.03.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.03.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.11', 'properties': {'start_datetime': '2016-01-14T10:54:00Z', 'end_datetime': '2016-01-14T11:00:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:22.863000Z', 'datetime': '2022-08-15T06:26:17.648000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.11.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.11.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.05', 'properties': {'start_datetime': '2016-01-14T10:18:00Z', 'end_datetime': '2016-01-14T10:24:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:22.649000Z', 'datetime': '2022-08-15T06:26:18.060000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.05.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.05.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.13', 'properties': {'start_datetime': '2016-01-14T11:06:00Z', 'end_datetime': '2016-01-14T11:12:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:22.277000Z', 'datetime': '2022-08-15T06:26:18.090000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.13.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.13.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}, {'type': 'Feature', 'stac_version': '1.0.0', 'id': 'SNDR.SNPP.ATMS.L1A.nominal2.02', 'properties': {'start_datetime': '2016-01-14T10:00:00Z', 'end_datetime': '2016-01-14T10:06:00Z', 'created': '2020-12-14T13:50:00Z', 'updated': '2022-08-15T06:26:22.169000Z', 'datetime': '2022-08-15T06:26:17.466000Z'}, 'geometry': {'type': 'Point', 'coordinates': [0.0, 0.0]}, 'links': [{'rel': 'collection', 'href': '.'}], 'assets': {'data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.02.nc', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.02.nc'}, 'metadata__data': {'href': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas'}, 'metadata__cmr': {'href': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml', 'title': 'SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml', 'description': 'SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml'}}, 'bbox': [0.0, 0.0, 0.0, 0.0], 'stac_extensions': [], 'collection': 'SNDR_SNPP_ATMS_L1A___1'}]
        os.environ['STAC_JSON'] = json.dumps(granule_json)
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('DOWNLOAD')
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['DOWNLOAD_DIR'] = tmp_dir_name
            download_result = choose_process()
            self.assertTrue(isinstance(download_result, list), f'download_result is not list: {download_result}')
            self.assertEqual(sum([len(k) for k in download_result]), len(glob(os.path.join(tmp_dir_name, '*'))), f'downloaded file does not match')
        return

    def test_03_upload(self):
        os.environ[Constants.USERNAME] = '/unity/uds/user/wphyo/username'
        os.environ[Constants.PASSWORD] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '6ir9qveln397i0inh9pmsabq1'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'
        os.environ['DAPA_API'] = 'https://58nbcawrvb.execute-api.us-west-2.amazonaws.com/test'
        os.environ['COLLECTION_ID'] = 'NEW_COLLECTION_EXAMPLE_L1B___9'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['STAGING_BUCKET'] = 'uds-test-cumulus-staging'

        os.environ['GRANULES_SEARCH_DOMAIN'] = 'UNITY'
        os.environ['GRANULES_UPLOAD_TYPE'] = 'S3'
        if len(argv) > 1:
            argv.pop(-1)
        argv.append('UPLOAD')

        with tempfile.TemporaryDirectory() as tmp_dir_name:
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
        catalog_result = choose_process()
        self.assertEqual('registered', catalog_result, 'wrong status')
        return
