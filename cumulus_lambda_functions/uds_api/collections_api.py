import logging
from typing import Union

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from cumulus_lambda_functions.uds_api.dapa.collections_dapa_cnm import CnmRequestBody, CollectionsDapaCnm
from cumulus_lambda_functions.uds_api.dapa.collections_dapa_creation import CollectionDapaCreation
from cumulus_lambda_functions.uds_api.dapa.collections_dapa_query import CollectionDapaQuery
from cumulus_lambda_functions.uds_api.dapa.pagination_links_generator import PaginationLinksGenerator
from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants

LOGGER = logging.getLogger(__name__)

router = APIRouter(
    prefix=f'/{WebServiceConstants.COLLECTIONS}',
    tags=["Process CRUD"],
    responses={404: {"description": "Not found"}},
)

@router.put("")
async def ingest_cnm_dapa(request: Request, new_cnm_body: CnmRequestBody):
    try:
        collections_dapa_cnm = CollectionsDapaCnm(new_cnm_body.model_dump())
        cnm_result = collections_dapa_cnm.start()
    except Exception as e:
        LOGGER.exception('failed during ingest_cnm_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if cnm_result['statusCode'] == 200:
        return cnm_result['body']
    raise HTTPException(status_code=cnm_result['statusCode'], detail=cnm_result['body'])


@router.post("")
async def create_new_collection(request: Request, new_collection: dict, response: Response):
    try:
        # new_collection = request.body()
        creation_result = CollectionDapaCreation(new_collection).start('NA')
    except Exception as e:
        LOGGER.exception('failed during ingest_cnm_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if creation_result['statusCode'] < 300:
        response.status_code = creation_result['statusCode']
        return creation_result['body']
    raise HTTPException(status_code=creation_result['statusCode'], detail=creation_result['body'])


@router.post("/actual")
async def create_new_collection(request: Request, new_collection: dict):
    try:
        creation_result = CollectionDapaCreation(new_collection).create()
    except Exception as e:
        LOGGER.exception('failed during ingest_cnm_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if creation_result['statusCode'] < 300:
        return creation_result['body'], creation_result['statusCode']
    raise HTTPException(status_code=creation_result['statusCode'], detail=creation_result['body'])

@router.get("")
async def query_collections(request: Request, collection_id: Union[str, None] = None, limit: Union[int, None] = 10, offset: Union[int, None] = 0, ):
    try:
        pagination_links = PaginationLinksGenerator(request).generate_pagination_links()
        collections_dapa_query = CollectionDapaQuery(collection_id, limit, offset, pagination_links)
        collections_result = collections_dapa_query.start()
    except Exception as e:
        LOGGER.exception('failed during get_granules_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if collections_result['statusCode'] == 200:
        return collections_result['body']
    raise HTTPException(status_code=collections_result['statusCode'], detail=collections_result['body'])
