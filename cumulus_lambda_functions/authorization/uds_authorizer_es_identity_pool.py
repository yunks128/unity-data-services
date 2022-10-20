import logging
import os

from cumulus_lambda_functions.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.aws.aws_cognito import AwsCognito
from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract
from cumulus_lambda_functions.lib.aws.es_factory import ESFactory

LOGGER = logging.getLogger(__name__)


class UDSAuthorizorEsIdentityPool(UDSAuthorizorAbstract):

    def __init__(self, user_pool_id: str) -> None:
        super().__init__()
        es_url = os.getenv('ES_URL')  # TODO validation
        authorization_index = os.getenv('AUTHORIZATION_URL')  # LDAP_Group_Permission
        es_port = int(os.getenv('ES_PORT', '443'))
        self.__cognito = AwsCognito(user_pool_id)
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=authorization_index,
                                                         base_url=es_url,
                                                         port=es_port)

    def authorize(self, username, resource, action) -> bool:
        belonged_groups = set(self.__cognito.get_groups(username))
        authorized_groups = self.__es.query({
            'query': {
                'match_all': {}  # TODO
            }
        })
        LOGGER.debug(f'belonged_groups for {username}: {belonged_groups}')
        authorized_groups = set([k['_source']['group_name'] for k in authorized_groups['hits']['hits']])
        LOGGER.debug(f'authorized_groups for {resource}-{action}: {authorized_groups}')
        if any([k in authorized_groups for k in belonged_groups]):
            LOGGER.debug(f'{username} is authorized for {resource}-{action}')
            return True
        LOGGER.debug(f'{username} is NOT authorized for {resource}-{action}')
        return False