from fastapi import APIRouter

from cumulus_lambda_functions.uds_api import collections_api, granules_api, auth_admin_api, system_admin_api

# from ideas_api.src.endpoints import job_endpoints
# from ideas_api.src.endpoints import process_endpoints
# from ideas_api.src.endpoints import setup_es


main_router = APIRouter(redirect_slashes=False)
# main_router.include_router(setup_es.router)
main_router.include_router(auth_admin_api.router)
main_router.include_router(system_admin_api.router)
main_router.include_router(collections_api.router)
main_router.include_router(granules_api.router)
