import os
from copy import deepcopy

from cumulus_lambda_functions.granules_to_es.granules_index_mapping import GranulesIndexMapping
from cumulus_lambda_functions.lib.time_utils import TimeUtils

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract

from cumulus_lambda_functions.lib.aws.es_factory import ESFactory

from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class CumulusDbIndex:
    def __init__(self):
        required_env = ['ES_URL']
        if not all([k in os.environ for k in required_env]):
            raise EnvironmentError(f'one or more missing env: {required_env}')
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=DBConstants.cumulus_alias,  # TODO should this come from setting?
                                                         base_url=os.getenv('ES_URL'),
                                                         port=int(os.getenv('ES_PORT', '443'))
                                                         )

    def delete_executions(self, cutoff_datetime):
        delete_result = self.__es.delete_by_query({
            "query": {
                "bool": {
                    "must": [
                        {"term": {
                            "_type": {
                                "value": "execution"
                            }
                        }},
                        {
                            "range": {
                                "updatedAt": {
                                    "lte": cutoff_datetime
                                }
                            }
                        }
                    ]
                }
            }
        })
        return delete_result
