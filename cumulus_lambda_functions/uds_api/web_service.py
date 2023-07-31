# import logging
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator
from dotenv import load_dotenv
from mangum.types import LambdaEvent, LambdaContext

load_dotenv()
LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())
# LOGGER = logging.getLogger(__name__)

import uvicorn
from fastapi import FastAPI
from mangum import Mangum
from starlette.requests import Request

from contextlib import ExitStack
from mangum.protocols import HTTPCycle, LifespanCycle

from cumulus_lambda_functions.uds_api.routes_api import main_router
from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants

app = FastAPI(title='Cryptocurrency API',
              description='API to track current prices and trading signals')
app.include_router(main_router, prefix=f'/{WebServiceConstants.API_PREFIX}')

@app.get("/")
async def root(request: Request):
    return {"message": "Hello World", "root_path": request.scope.get("root_path")}


class MyMangum(Mangum):

    def __call__(self, event: LambdaEvent, context: LambdaContext) -> dict:
        LOGGER.warn(f'it is not even here?: {event} ')
        try:
            chosen_handler = self.infer(event, context)
            LOGGER.info(f'{type(chosen_handler)}')
            with ExitStack() as stack:
                LOGGER.info(f'lifespan: {self.lifespan}')
                if self.lifespan in ("auto", "on"):
                    LOGGER.info(f'lifespan: {self.lifespan}')
                    lifespan_cycle = LifespanCycle(self.app, self.lifespan)
                    stack.enter_context(lifespan_cycle)
                LOGGER.info(f'starting httpCycle: {chosen_handler.scope}.. {chosen_handler.body}')
                http_cycle = HTTPCycle(chosen_handler.scope, chosen_handler.body)
                LOGGER.info(f'calling it?')
                http_response = http_cycle(self.app)
                LOGGER.info(f'http_response {http_response}')

                return chosen_handler(http_response)
        except Exception as e:
            LOGGER.exception(f'error in mangum.')
            raise e

# to make it work with Amazon Lambda, we create a handler object
handler = Mangum(app=app)

if __name__ == '__main__':
    uvicorn.run("web_service:app", port=8005, log_level="info", reload=True)
    print("running")