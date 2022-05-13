from functools import wraps

from flask import request

from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_abstract import AuthenticatorAbstract
from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_error import AuthenticatorError
from cumulus_lambda_functions.cumulus_dapa.authenticator.authenticator_factory import AuthenticatorFactory


def authenticator_decorator(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        authenticator: AuthenticatorAbstract = AuthenticatorFactory().get_instance(AuthenticatorFactory.FILE)
        try:
            auth_result = authenticator.authenticate(request.headers)
            kwargs['auth_jwt_token'] = auth_result
        except AuthenticatorError as authenticator_err:
            return {'message': f'failed while attempting to authenticate', 'details': str(authenticator_err)}, 403
        except Exception as e:
            return {'message': f'failed while attempting to authenticate', 'details': str(e)}, 500
        return f(*args, **kwargs)
    return decorated_function
