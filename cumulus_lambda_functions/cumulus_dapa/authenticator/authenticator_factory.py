from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_filebased import AuthenticatorFileBased
from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_pass_through import AuthenticatorPassThrough


class AuthenticatorFactory:
    AWS = 'AWS'
    FILE = 'FILE'
    PASS_THROUGH = 'PASS_THROUGH'

    def get_instance(self, class_type: str):
        if class_type == self.FILE:
            return AuthenticatorFileBased()
        if class_type == self.PASS_THROUGH:
            return AuthenticatorPassThrough()
        raise ValueError(f'invalid class type: {class_type}')
