import json
import os
from time import time
from typing import Union

from starlette.responses import Response, RedirectResponse
from cumulus_lambda_functions.uds_api.fast_api_utils import FastApiUtils

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

from fastapi import APIRouter, HTTPException, Request
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
async def stac_entry(request: Request, response: Response):
    request_headers = dict(request.headers)
    LOGGER.debug(f'stac_entry - request_headers: {request_headers}')
    print(request_headers)
    base_url = os.environ.get(WebServiceConstants.BASE_URL, f'{request.url.scheme}://{request.url.netloc}')
    base_url = base_url[:-1] if base_url.endswith('/') else base_url
    base_url = base_url if base_url.startswith('http') else f'https://{base_url}'
    api_base_prefix = FastApiUtils.get_api_base_prefix()
    ending_url = f'{WebServiceConstants.STAC_BROWSER}/' if str(request.url).endswith('/') else WebServiceConstants.STAC_BROWSER
    redirect_response = RedirectResponse(f'/{api_base_prefix}/{ending_url}')
    if 'oidc_access_token' in request_headers:
        redirect_response.set_cookie(key="unity_token", value=request_headers['oidc_access_token'], httponly=False, secure=False, samesite='strict')  # missing , domain=base_url
        redirect_response.set_cookie(key="test1", value=f"{time()}", httponly=False, secure=False, samesite='strict')  # missing , domain=base_url
    return redirect_response
