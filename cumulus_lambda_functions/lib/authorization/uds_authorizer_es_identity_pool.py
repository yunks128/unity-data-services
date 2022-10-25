import logging
import os

from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.aws.aws_cognito import AwsCognito
from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract
from cumulus_lambda_functions.lib.aws.es_factory import ESFactory
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants

LOGGER = logging.getLogger(__name__)


class UDSAuthorizorEsIdentityPool(UDSAuthorizorAbstract):

    def __init__(self, user_pool_id: str) -> None:
        super().__init__()
        es_url = os.getenv('ES_URL')  # TODO validation
        self.__authorization_index = os.getenv('AUTHORIZATION_INDEX')  # LDAP_Group_Permission
        es_port = int(os.getenv('ES_PORT', '443'))
        self.__cognito = AwsCognito(user_pool_id)
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=self.__authorization_index,
                                                         base_url=es_url,
                                                         port=es_port)

    def add_authorized_group(self, action: [str], project: str, venue: str, ldap_group_name: str):
        self.__es.index_one({
            DBConstants.action_key: action,
            DBConstants.project: project,
            DBConstants.project_venue: venue,
            DBConstants.authorized_group_name_key: ldap_group_name,
        }, f'{project}__{venue}__{ldap_group_name}', self.__authorization_index)
        return

    def delete_authorized_group(self, project: str, venue: str, ldap_group_name: str):
        self.__es.delete_by_query({
            'query': {
                'bool': {
                    'must': [
                        {'term': {DBConstants.project: project}},
                        {'term': {DBConstants.project_venue: venue}},
                        {'term': {DBConstants.authorized_group_name_key: ldap_group_name}},
                    ]
                }
            }
        })
        return

    def list_authorized_groups_for(self, project: str, venue: str):
        result = self.__es.query_pages({
            'query': {
                'bool': {
                    'must': [
                        {'term': {DBConstants.project: project}},
                        {'term': {DBConstants.project_venue: venue}},
                    ]
                }
            },
            'sort': [
                {DBConstants.project: {'order': 'asc'}},
                {DBConstants.project_venue: {'order': 'asc'}},
                {DBConstants.authorized_group_name_key: {'order': 'asc'}},
            ]
        })
        result = [k['_source'] for k in result['hits']['hits']]
        return result

    def update_authorized_group(self, action: [str], project: str, venue: str, ldap_group_name: str):
        self.__es.update_one({
            DBConstants.action_key: action,
            DBConstants.project: project,
            DBConstants.project_venue: venue,
            DBConstants.authorized_group_name_key: ldap_group_name,
        }, f'{project}__{venue}__{ldap_group_name}', self.__authorization_index)
        return

    def get_authorized_tenant(self, username: str, action: str) -> list:
        belonged_groups = set(self.__cognito.get_groups(username))

        authorized_groups = self.__es.query({
            'query': {
                'bool': {
                    'must': [
                        {
                            'terms': {
                                DBConstants.authorized_group_name_key: list(belonged_groups),
                            }
                        },
                        {
                            'term': {
                                DBConstants.action_key: action,
                            }
                        }
                    ]
                }
            }
        })
        return [k['_source'] for k in authorized_groups['hits']['hits']]

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
