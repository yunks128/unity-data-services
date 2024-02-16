import os

from cumulus_lambda_functions.lib.time_utils import TimeUtils

from cumulus_lambda_functions.cleanup_executions.cumulus_db_index import CumulusDbIndex


class ExecutionDocsCleaner:
    def __init__(self, event):
        self.__event = event

    def start(self):
        cumulus_db = CumulusDbIndex()
        relative_time = int(os.environ.get('CUT_OFF_DAYS', 14))
        cut_off_time = TimeUtils().get_current_unix_milli() - TimeUtils.DAY_IN_MILLISECOND * relative_time
        cumulus_db.delete_executions(cut_off_time)
        return
