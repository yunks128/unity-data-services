from typing import Union

from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_abstract import AuthenticatorAbstract
from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_error import AuthenticatorError


class AuthenticatorFileBased(AuthenticatorAbstract):
    __auth_cred_key = 'auth_cred'

    def __init__(self) -> None:
        super().__init__()
        self.__bearer_key = 'Bearer'
        self.__auth_key = 'Authorization'

    def authenticate(self, input_auth_cred: dict) -> Union[str, None]:
        if self.__auth_key not in input_auth_cred:
            raise AuthenticatorError('missing key: Authorization')
        potential_auth_key = input_auth_cred[self.__auth_key]
        if not potential_auth_key.startswith(self.__bearer_key):
            raise AuthenticatorError(f'{self.__auth_key} Key does not start with `{self.__bearer_key}`')
        potential_auth_key = potential_auth_key[len(self.__bearer_key):].strip()
        return potential_auth_key
