import os

from fastapi.openapi.utils import get_openapi

from cumulus_lambda_functions.lib.constants import Constants

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from dotenv import load_dotenv

load_dotenv()

import uvicorn
from fastapi import FastAPI
from mangum import Mangum
from starlette.requests import Request

from cumulus_lambda_functions.uds_api.routes_api import main_router
from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())

api_base_prefix = os.environ.get(Constants.DAPA_API_PREIFX_KEY) if Constants.DAPA_API_PREIFX_KEY in os.environ else WebServiceConstants.API_PREFIX
app = FastAPI(title='Unity UDS API',
              description='API to interact with UDS services',
              docs_url=f'/{api_base_prefix}/docs',
              redoc_url=f'/{api_base_prefix}/redoc',
              openapi_url=f'/{api_base_prefix}/docs/openapi',
              )
app.include_router(main_router, prefix=f'/{api_base_prefix}')

@app.get("/")
async def root(request: Request):
    return {"message": "Hello World", "root_path": request.scope.get("root_path")}


@app.get(f'/{api_base_prefix}/openapi')
@app.get(f'/{api_base_prefix}/openapi/')
async def get_open_api(request: Request):
    default_open_api_doc = app.openapi()
    dropping_keys = [k for k in default_open_api_doc['paths'].keys() if not k.endswith('/')]
    for k in dropping_keys:
        default_open_api_doc['paths'].pop(k)
    return app.openapi()


# to make it work with Amazon Lambda, we create a handler object
handler = Mangum(app=app)

if __name__ == '__main__':
    uvicorn.run("web_service:app", port=8005, log_level="info", reload=True)
    print("running")
