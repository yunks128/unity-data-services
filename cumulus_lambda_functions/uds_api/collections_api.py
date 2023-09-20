from typing import Union

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from fastapi import APIRouter, HTTPException, Request, Response

from cumulus_lambda_functions.uds_api.dapa.collections_dapa_cnm import CnmRequestBody, CollectionsDapaCnm
from cumulus_lambda_functions.uds_api.dapa.collections_dapa_creation import CollectionDapaCreation
from cumulus_lambda_functions.uds_api.dapa.collections_dapa_query import CollectionDapaQuery
from cumulus_lambda_functions.uds_api.dapa.pagination_links_generator import PaginationLinksGenerator
from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())

router = APIRouter(
    prefix=f'/{WebServiceConstants.COLLECTIONS}',
    tags=["Process CRUD"],
    responses={404: {"description": "Not found"}},
)

@router.put("")
@router.put("/")
async def ingest_cnm_dapa(request: Request, new_cnm_body: CnmRequestBody, response: Response):
    LOGGER.debug(f'starting ingest_cnm_dapa')
    try:
        cnm_prep_result = CollectionsDapaCnm(new_cnm_body.model_dump()).start_facade(request.url)
    except Exception as e:
        LOGGER.exception('failed during ingest_cnm_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if cnm_prep_result['statusCode'] < 300:
        response.status_code = cnm_prep_result['statusCode']
        return cnm_prep_result['body']
    raise HTTPException(status_code=cnm_prep_result['statusCode'], detail=cnm_prep_result['body'])


@router.put("/actual")
@router.put("/actual/")
async def ingest_cnm_dapa_actual(request: Request, new_cnm_body: CnmRequestBody):
    LOGGER.debug(f'starting ingest_cnm_dapa_actual')
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
@router.post("/")
async def create_new_collection(request: Request, new_collection: dict, response: Response):
    LOGGER.debug(f'starting create_new_collection')
    try:
        # new_collection = request.body()
        creation_result = CollectionDapaCreation(new_collection).start(request.url)
    except Exception as e:
        LOGGER.exception('failed during ingest_cnm_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if creation_result['statusCode'] < 300:
        response.status_code = creation_result['statusCode']
        return creation_result['body']
    raise HTTPException(status_code=creation_result['statusCode'], detail=creation_result['body'])


@router.post("/actual")
async def create_new_collection_real(request: Request, new_collection: dict):
    LOGGER.debug(f'starting create_new_collection_real')
    try:
        creation_result = CollectionDapaCreation(new_collection).create()
    except Exception as e:
        LOGGER.exception('failed during ingest_cnm_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if creation_result['statusCode'] < 300:
        return creation_result['body'], creation_result['statusCode']
    raise HTTPException(status_code=creation_result['statusCode'], detail=creation_result['body'])


@router.get("")
@router.get("/")
@router.get("/{collection_id}")
@router.get("/{collection_id}/")
async def query_collections(request: Request, collection_id: Union[str, None] = None, limit: Union[int, None] = 10, offset: Union[int, None] = 0, ):
    LOGGER.debug(f'starting query_collections: {collection_id}')
    try:
        if limit > CollectionDapaQuery.max_limit:
            LOGGER.debug(f'incoming limit > {CollectionDapaQuery.max_limit}. resetting to max. incoming limit: {limit}')
            limit = CollectionDapaQuery.max_limit
        print(f'new limit: {limit}')
        pagination_links = PaginationLinksGenerator(request).generate_pagination_links()
        collections_dapa_query = CollectionDapaQuery(collection_id, limit, offset, pagination_links)
        collections_result = collections_dapa_query.start()
    except Exception as e:
        LOGGER.exception('failed during get_granules_dapa')
        raise HTTPException(status_code=500, detail=str(e))
    if collections_result['statusCode'] == 200:
        return collections_result['body']
    raise HTTPException(status_code=collections_result['statusCode'], detail=collections_result['body'])