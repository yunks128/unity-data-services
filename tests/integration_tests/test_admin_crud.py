import base64
import json
import os
from datetime import datetime
from unittest import TestCase

import requests
from dotenv import load_dotenv

from cumulus_lambda_functions.lib.cognito_login.cognito_login import CognitoLogin


class TestDapaStac(TestCase):

    def test_setup_es(self):
        load_dotenv()
        cognito_login = CognitoLogin() \
            .with_client_id(os.environ.get('CLIENT_ID', '')) \
            .with_cognito_url(os.environ.get('COGNITO_URL', '')) \
            .with_verify_ssl(False) \
            .start(base64.standard_b64decode(os.environ.get('USERNAME')).decode(),
                   base64.standard_b64decode(os.environ.get('PASSWORD')).decode())
        es_setup_url = f'{os.environ.get("UNITY_URL")}/am-uds-dapa/admin/system/es_setup/'
        s = requests.session()
        s.trust_env = False
        response = s.put(url=es_setup_url, headers={'Authorization': f'Bearer {cognito_login.token}'}, verify=False)
        return

    def test_list_admin_list_01(self):
        load_dotenv()
        cognito_login = CognitoLogin()\
            .with_client_id(os.environ.get('CLIENT_ID', ''))\
            .with_cognito_url(os.environ.get('COGNITO_URL', ''))\
            .with_verify_ssl(False)\
            .start(base64.standard_b64decode(os.environ.get('USERNAME')).decode(), base64.standard_b64decode(os.environ.get('PASSWORD')).decode())
        collection_url = f'{os.environ.get("UNITY_URL")}/am-uds-dapa/admin/auth'
        s = requests.session()
        s.trust_env = False
        response = s.get(url=collection_url, headers={'Authorization': f'Bearer {cognito_login.token}'}, verify=False)
        self.assertEqual(response.status_code, 200, f'wrong status code: {response.text}')
        response_json = json.loads(response.content.decode())
        print(json.dumps(response_json, indent=4))
        return

    def test_add_admin_01(self):
        load_dotenv()
        cognito_login = CognitoLogin()\
            .with_client_id(os.environ.get('CLIENT_ID', ''))\
            .with_cognito_url(os.environ.get('COGNITO_URL', ''))\
            .with_verify_ssl(False)\
            .start(base64.standard_b64decode(os.environ.get('USERNAME')).decode(), base64.standard_b64decode(os.environ.get('PASSWORD')).decode())
        collection_url = f'{os.environ.get("UNITY_URL")}/am-uds-dapa/admin/auth'
        admin_add_body = {
            "actions": ["READ", "CREATE"],
            "resources": ["urn:nasa:unity:uds_local_test:DEV1:.*"],
            "tenant": "uds_local_test",
            "venue": f"DEV1-{int(datetime.utcnow().timestamp())}",
            # "venue": f"DEV1",
            "group_name": "Unity_Viewer"
        }
        response = requests.put(url=collection_url, headers={
            'Authorization': f'Bearer {cognito_login.token}',
            'Content-Type': 'application/json',
        }, verify=False, data=json.dumps(admin_add_body))
        self.assertEqual(response.status_code, 200, f'wrong status code: {response.text}')
        response_json = response.content.decode()
        print(response_json)
        return

    def test_delete_admin_01(self):
        load_dotenv()
        cognito_login = CognitoLogin()\
            .with_client_id(os.environ.get('CLIENT_ID', ''))\
            .with_cognito_url(os.environ.get('COGNITO_URL', ''))\
            .with_verify_ssl(False)\
            .start(base64.standard_b64decode(os.environ.get('USERNAME')).decode(), base64.standard_b64decode(os.environ.get('PASSWORD')).decode())
        collection_url = f'{os.environ.get("UNITY_URL")}/am-uds-dapa/admin/auth'
        admin_add_body = {
            "tenant": "uds_local_test",
            "venue": "DEV1-1674680116",
            "group_name": "Unity_Viewer"
        }
        response = requests.delete(url=collection_url, headers={
            'Authorization': f'Bearer {cognito_login.token}',
            'Content-Type': 'application/json',
        }, verify=False, data=json.dumps(admin_add_body))
        self.assertEqual(response.status_code, 200, f'wrong status code: {response.text}')
        response_json = response.content.decode()
        print(response_json)
        return

