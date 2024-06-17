import json
import os
from time import time
from typing import Union

from starlette.responses import Response, RedirectResponse

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
    prefix=f'/{WebServiceConstants.MISC}',
    tags=["Granules CRUD API"],
    responses={404: {"description": "Not found"}},
)

# https://docs.ogc.org/per/20-025r1.html#_get_collectionscollectionidvariables
@router.get("/{collection_id}/variables")
@router.get("/{collection_id}/variables/")
async def get_granules_dapa(request: Request, collection_id: str):
    return

@router.get(f'/stac_entry')
@router.get(f'/stac_entry/')
async def stacc_entry(request: Request, response: Response):
    base_url = os.environ.get(WebServiceConstants.BASE_URL, f'{request.url.scheme}://{request.url.netloc}')
    base_url = base_url[:-1] if base_url.endswith('/') else base_url
    base_url = base_url if base_url.startswith('http') else f'https://{base_url}'
    api_base_prefix = FastApiUtils.get_api_base_prefix()
    ending_url = f'{WebServiceConstants.STAC_BROWSER}/' if str(request.url).endswith('/') else WebServiceConstants.STAC_BROWSER
    # response.set_cookie(key="unity_token", value=f"fake-cookie-session-value-{time()}", httponly=True, secure=True, samesite='strict')  # missing , domain=base_url
    # response.set_cookie(key="unity_token", value=f"fake-cookie-session-value-{time()}")
    redirect_response = RedirectResponse(f'/{api_base_prefix}/{ending_url}')
    redirect_response.set_cookie(key="unity_token", value=os.environ.get('TEMP_TOKEN', ''), httponly=False, secure=False, samesite='strict')  # missing , domain=base_url
    redirect_response.set_cookie(key="test1", value=f"{time()}", httponly=False, secure=False, samesite='strict')  # missing , domain=base_url
    return redirect_response
    # return {'hello': 1}