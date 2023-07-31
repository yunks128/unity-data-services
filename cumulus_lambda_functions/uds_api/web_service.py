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

app = FastAPI(title='Cryptocurrency API',
              description='API to track current prices and trading signals')
app.include_router(main_router, prefix=f'/{WebServiceConstants.API_PREFIX}')

@app.get("/")
async def root(request: Request):
    return {"message": "Hello World", "root_path": request.scope.get("root_path")}


# to make it work with Amazon Lambda, we create a handler object
handler = Mangum(app=app)

if __name__ == '__main__':
    uvicorn.run("web_service:app", port=8005, log_level="info", reload=True)
    print("running")