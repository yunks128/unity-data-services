import logging
import os
import re

from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract
from cumulus_lambda_functions.lib.aws.aws_cognito import AwsCognito
from cumulus_lambda_functions.lib.aws.es_abstract import ESAbstract
from cumulus_lambda_functions.lib.aws.es_factory import ESFactory
from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants

LOGGER = logging.getLogger(__name__)


class UDSAuthorizorEsIdentityPool(UDSAuthorizorAbstract):

    def __init__(self, user_pool_id: str, es_url: str, es_port=443) -> None:
        super().__init__()
        self.__cognito = AwsCognito(user_pool_id)
        self.__es: ESAbstract = ESFactory().get_instance('AWS',
                                                         index=DBConstants.authorization_index,
                                                         base_url=es_url,
                                                         port=es_port)

    def add_authorized_group(self, action: [str], resource: [str], tenant: str, venue: str, ldap_group_name: str):
        self.__es.index_one({
            DBConstants.action_key: action,
            DBConstants.resource_key: resource,
            DBConstants.tenant: tenant,
            DBConstants.tenant_venue: venue,
            DBConstants.authorized_group_name_key: ldap_group_name,
        }, f'{tenant}__{venue}__{ldap_group_name}', DBConstants.authorization_index)
        return

    def delete_authorized_group(self, tenant: str, venue: str, ldap_group_name: str):
        self.__es.delete_by_query({
            'query': {
                'bool': {
                    'must': [
                        {'term': {DBConstants.tenant: tenant}},
                        {'term': {DBConstants.tenant_venue: venue}},
                        {'term': {DBConstants.authorized_group_name_key: ldap_group_name}},
                    ]
                }
            }
        })
        return

    def list_authorized_groups_for(self, tenant: str, venue: str):
        result = self.__es.query_pages({
            'query': {
                'bool': {
                    'must': [
                        {'term': {DBConstants.tenant: tenant}},
                        {'term': {DBConstants.tenant_venue: venue}},
                    ]
                }
            },
            'sort': [
                {DBConstants.tenant: {'order': 'asc'}},
                {DBConstants.tenant_venue: {'order': 'asc'}},
                {DBConstants.authorized_group_name_key: {'order': 'asc'}},
            ]
        })
        result = [k['_source'] for k in result['hits']['hits']]
        return result

    def update_authorized_group(self, action: [str], resource: [str], tenant: str, venue: str, ldap_group_name: str):
        self.__es.update_one({
            DBConstants.action_key: action,
            DBConstants.resource_key: resource,
            DBConstants.tenant: tenant,
            DBConstants.tenant_venue: venue,
            DBConstants.authorized_group_name_key: ldap_group_name,
        }, f'{tenant}__{venue}__{ldap_group_name}', DBConstants.authorization_index)
        return

    def get_authorized_tenant(self, username: str, action: str, resource: str) -> list:
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
                        },
                        {
                            'term': {
                                DBConstants.resource_key: resource,
                            }
                        }
                    ]
                }
            }
        })
        return [k['_source'] for k in authorized_groups['hits']['hits']]

    def is_authorized_for_collection(self, action: str, collection_id: str, username: str, tenant: str, venue: str):
        belonged_groups = set(self.__cognito.get_groups(username))
        authorization_dsl = {
            '_source': [DBConstants.resource_key],
            'size': 9999,
            'query': {
                'bool': {
                    'must': [
                        {'terms': {DBConstants.authorized_group_name_key: list(belonged_groups)}},
                        {'term': {DBConstants.action_key: action}},
                        {'term': {DBConstants.tenant: tenant}},
                        {'term': {DBConstants.tenant_venue: venue}},
                    ]
                }
            }
        }
        authorized_collection_map = self.__es.query(authorization_dsl)
        authorized_collection_map = [k['_source'][DBConstants.resource_key] for k in authorized_collection_map['hits']['hits']]
        authorized_collection_map = [item for sublist in authorized_collection_map for item in sublist]
        for each_collection_regex in authorized_collection_map:
            LOGGER.debug(f'comparing regex: {each_collection_regex} vs {collection_id}')
            version_regex = re.compile(each_collection_regex)
            if version_regex.match(collection_id):
                return True
        return False

    def get_authorized_collections(self, action: str, username: str, tenant: str = '', venue: str = ''):
        belonged_groups = set(self.__cognito.get_groups(username))
        authorization_dsl = {
            '_source': [DBConstants.resource_key],
            'size': 9999,
            'query': {
                'bool': {
                    'must': [
                        {'terms': {DBConstants.authorized_group_name_key: list(belonged_groups)}},
                        {'term': {DBConstants.action_key: action}},
                    ]
                }
            }
        }
        if tenant != '':
            authorization_dsl['query']['bool']['must'].append({'term': {DBConstants.tenant: tenant}})
        if venue != '':
            authorization_dsl['query']['bool']['must'].append({'term': {DBConstants.tenant_venue: venue}})
        authorized_collection_map = self.__es.query(authorization_dsl)
        authorized_collection_map = [k['_source'][DBConstants.resource_key] for k in authorized_collection_map['hits']['hits']]
        authorized_collection_map = [item for sublist in authorized_collection_map for item in sublist]
        return authorized_collection_map

