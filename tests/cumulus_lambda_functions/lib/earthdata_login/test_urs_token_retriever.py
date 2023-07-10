import os
from unittest import TestCase

from cumulus_lambda_functions.lib.constants import Constants
from cumulus_lambda_functions.lib.earthdata_login.urs_token_retriever import URSTokenRetriever


class TestURSTokenRetriever(TestCase):
    def test_01(self):
        os.environ[Constants.EDL_USERNAME] = '/unity/uds/user/wphyo/edl_username'
        os.environ[Constants.EDL_PASSWORD] = '/unity/uds/user/wphyo/edl_dwssap'
        # os.environ[Constants.USERNAME] = 'usps_username'
        # os.environ[Constants.PASSWORD] = 'usps_password'
        os.environ[Constants.EDL_PASSWORD_TYPE] = Constants.PARAM_STORE
        os.environ[Constants.EDL_BASE_URL] = 'urs1.earthdata.nasa.gov'
        result = URSTokenRetriever().start()
        self.assertTrue(len(result) > 0, 'empty token')
        return
