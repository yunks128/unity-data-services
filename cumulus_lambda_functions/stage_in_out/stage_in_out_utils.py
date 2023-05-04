import json
import logging
import os
from typing import Union

from cumulus_lambda_functions.lib.utils.file_utils import FileUtils

LOGGER = logging.getLogger(__name__)


class StageInOutUtils:
    OUTPUT_FILE = 'OUTPUT_FILE'

    @staticmethod
    def write_output_to_file(output_json: Union[dict, str, list]):
        if StageInOutUtils.OUTPUT_FILE not in os.environ:
            LOGGER.debug(f'Not writing output to file due to missing {StageInOutUtils.OUTPUT_FILE} in ENV')
            return
        output_filepath = os.environ.get(StageInOutUtils.OUTPUT_FILE)
        FileUtils.mk_dir_p(os.path.dirname(output_filepath))
        output_str = json.dumps(output_json) if not isinstance(output_json, str) else output_json
        with open(output_filepath, 'w') as ff:
            ff.write(output_str)
        return
