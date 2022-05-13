from typing import Union

from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_abstract import AuthenticatorAbstract


class AuthenticatorPassThrough(AuthenticatorAbstract):
    def authenticate(self, input_auth_cred: dict) -> Union[str, None]:
        return None
