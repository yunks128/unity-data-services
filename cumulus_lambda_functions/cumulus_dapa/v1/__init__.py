import os

from flask import Blueprint
from flask_restx import Api

from .granules import api as granules_api

_version = "1.0"
flask_prefix: str = os.environ.get('flask_prefix', '')
flask_prefix = flask_prefix if flask_prefix.startswith('/') else f'/{flask_prefix}'
flask_prefix = flask_prefix if flask_prefix.endswith('/') else f'{flask_prefix}/'

blueprint = Blueprint('parquet_flask', __name__, url_prefix=f'{flask_prefix}{_version}')
api = Api(blueprint,
          title='Parquet ingestion & query',
          version=_version,
          description='API to support the Parquet ingestion & query data',
          doc='/doc/'
          )

# Register namespaces
api.add_namespace(granules_api)

