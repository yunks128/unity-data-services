import os
import tempfile
from glob import glob
from unittest import TestCase

from cumulus_lambda_functions.cumulus_download_granules.download_granules import DownloadGranules


class TestDownloadGranules(TestCase):
    def test_01(self):
        os.environ['DAPA_API'] = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev'
        os.environ['DAPA_API'] = 'https://1gp9st60gd.execute-api.us-west-2.amazonaws.com/dev'  # mcp-dev
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['CLIENT_ID'] = '7a1fglm2d54eoggj13lccivp25'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'  # mcp-dev
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['COLLECTION_ID'] = 'urn:nasa:unity:uds_local_test:DEV1:CUMULUS_DAPA_UNIT_TEST___1674695854'
        # os.environ['DOWNLOAD_DIR'] = '/etc/granules'
        os.environ['VERIFY_SSL'] = 'FALSE'
        os.environ['LIMITS'] = '100'
        os.environ['LOG_LEVEL'] = '20'
        os.environ['DATE_FROM'] = '2016-01-14T10:00:00.000Z'
        os.environ['DATE_TO'] = '2016-01-15T10:06:00.000Z'

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            os.environ['DOWNLOAD_DIR'] = tmp_dir_name
            DownloadGranules().start()
            raw_files = glob(f'{tmp_dir_name}/*', recursive=True)
            self.assertEqual(3, len(raw_files), f'wrong file count: {raw_files}')
        return
