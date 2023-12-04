import json
import os
from typing import Union

from cumulus_lambda_functions.uds_api.dapa.granules_dapa_query_es import GranulesDapaQueryEs
from cumulus_lambda_functions.uds_api.dapa.granules_db_index import GranulesDbIndex
from cumulus_lambda_functions.uds_api.fast_api_utils import FastApiUtils

from cumulus_lambda_functions.lib.authorization.uds_authorizer_abstract import UDSAuthorizorAbstract

from cumulus_lambda_functions.lib.authorization.uds_authorizer_factory import UDSAuthorizerFactory

from cumulus_lambda_functions.lib.uds_db.db_constants import DBConstants

from cumulus_lambda_functions.lib.uds_db.uds_collections import UdsCollections

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from fastapi import APIRouter, HTTPException, Request

from cumulus_lambda_functions.uds_api.dapa.granules_dapa_query import GranulesDapaQuery
from cumulus_lambda_functions.uds_api.dapa.pagination_links_generator import PaginationLinksGenerator
from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())

router = APIRouter(
    prefix=f'/{WebServiceConstants.COLLECTIONS}',
    tags=["Granules CRUD API"],
    responses={404: {"description": "Not found"}},
)

# https://docs.ogc.org/per/20-025r1.html#_get_collectionscollectionidvariables
@router.get("/{collection_id}/variables")
@router.get("/{collection_id}/variables/")
async def get_granules_dapa(request: Request, collection_id: str):
    authorizer: UDSAuthorizorAbstract = UDSAuthorizerFactory() \
        .get_instance(UDSAuthorizerFactory.cognito,
                      es_url=os.getenv('ES_URL'),
                      es_port=int(os.getenv('ES_PORT', '443'))
                      )
    auth_info = FastApiUtils.get_authorization_info(request)
    collection_identifier = UdsCollections.decode_identifier(collection_id)
    if not authorizer.is_authorized_for_collection(DBConstants.read, collection_id,
                                                   auth_info['ldap_groups'],
                                                   collection_identifier.tenant,
                                                   collection_identifier.venue):
        LOGGER.debug(f'user: {auth_info["username"]} is not authorized for {collection_id}')
        raise HTTPException(status_code=403, detail=json.dumps({
            'message': 'not authorized to execute this action'
        }))

    try:
        granules_db_index = GranulesDbIndex()
        granule_index_mapping = granules_db_index.get_latest_index(collection_identifier.tenant, collection_identifier.venue)
        # This is the response from the method
        # {"unity_granule_main_project1694791693139_dev__v02":{"mappings":{"dynamic":"strict","properties":{"collection_id":{"type":"keyword"},"event_time":{"type":"long"},"granule_id":{"type":"keyword"},"last_updated":{"type":"long"},"tag":{"type":"keyword"}}}}}
        # needs to drill down to properties
        custom_metadata = granules_db_index.get_custom_metadata_fields(granule_index_mapping)
    except Exception as e:
        LOGGER.exception('failed during get_granules_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    return custom_metadata


@router.get("/{collection_id}/items")
@router.get("/{collection_id}/items/")
async def get_granules_dapa(request: Request, collection_id: str, limit: Union[int, None] = 10, offset: Union[str, None] = None, datetime: Union[str, None] = None, filter: Union[str, None] = None):
    authorizer: UDSAuthorizorAbstract = UDSAuthorizerFactory() \
        .get_instance(UDSAuthorizerFactory.cognito,
                      es_url=os.getenv('ES_URL'),
                      es_port=int(os.getenv('ES_PORT', '443'))
                      )
    auth_info = FastApiUtils.get_authorization_info(request)
    collection_identifier = UdsCollections.decode_identifier(collection_id)
    if not authorizer.is_authorized_for_collection(DBConstants.read, collection_id,
                                                   auth_info['ldap_groups'],
                                                   collection_identifier.tenant,
                                                   collection_identifier.venue):
        LOGGER.debug(f'user: {auth_info["username"]} is not authorized for {collection_id}')
        raise HTTPException(status_code=403, detail=json.dumps({
            'message': 'not authorized to execute this action'
        }))

    try:
        # pagination_links = PaginationLinksGenerator(request).generate_pagination_links()
        pagination_links = PaginationLinksGenerator(request)

        granules_dapa_query = GranulesDapaQueryEs(collection_id, limit, offset, datetime, filter, pagination_links)
        granules_result = granules_dapa_query.start()
    except Exception as e:
        LOGGER.exception('failed during get_granules_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if granules_result['statusCode'] == 200:
        return granules_result['body']
    raise HTTPException(status_code=granules_result['statusCode'], detail=granules_result['body'])
