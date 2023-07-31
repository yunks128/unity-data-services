import base64
import json

from fastapi import APIRouter, HTTPException, Request, Response


class FastApiUtils:
    @staticmethod
    def get_authorization_info(request: Request):
        """
        :return:
        """
        action = request.method
        resource = request.url.path
        bearer_token = request.headers.get('Authorization', '')
        username_part = bearer_token.split('.')[1]
        jwt_decoded = base64.standard_b64decode(f'{username_part}========'.encode()).decode()
        jwt_decoded = json.loads(jwt_decoded)
        ldap_groups = jwt_decoded['cognito:groups']
        username = jwt_decoded['username']
        return {
            'username': username,
            'ldap_groups': list(set(ldap_groups)),
            'action': action,
            'resource': resource,
        }
