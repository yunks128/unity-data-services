import os

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.time_utils import TimeUtils

from cumulus_lambda_functions.cleanup_executions.cumulus_db_index import CumulusDbIndex
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class ExecutionDocsCleaner:
    def __init__(self, event):
        self.__event = event

    def start(self):
        cumulus_db = CumulusDbIndex()
        relative_time = int(os.environ.get('CUT_OFF_DAYS', 14))
        cut_off_time = TimeUtils().get_current_unix_milli() - TimeUtils.DAY_IN_MILLISECOND * relative_time
        result = cumulus_db.delete_executions(cut_off_time)
        LOGGER.debug(f'deletion result for {cut_off_time}: {result}')
        return
