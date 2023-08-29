from cumulus_lambda_functions.cumulus_auth_crud.auth_crud import AuthCrud
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator


def auth_list(event, context):
    LambdaLoggerGenerator.remove_default_handlers()
    return AuthCrud(event).list_all_record()


def auth_add(event, context):
    LambdaLoggerGenerator.remove_default_handlers()
    return AuthCrud(event).add_new_record()


def auth_update(event, context):
    LambdaLoggerGenerator.remove_default_handlers()
    return AuthCrud(event).update_record()


def auth_delete(event, context):
    LambdaLoggerGenerator.remove_default_handlers()
    return AuthCrud(event).delete_record()
