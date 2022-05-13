# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
from copy import deepcopy

from flask_restx import Resource, Namespace, fields
from flask import request

from cumulus_lambda_functions.cumulus_dapa.v1.authenticator_decorator import authenticator_decorator
from cumulus_lambda_functions.cumulus_wrapper.query_granules import GranulesQuery

api = Namespace('collections/observation/items', description="Querying data")

LOGGER = logging.getLogger(__name__)

query_model = api.model('extract_stats', {
    'bbox': fields.String(required=False, example='0,0,10,10'),
    'datetime': fields.String(required=False, example='2018-02-12T00:00:00Z OR  2018-02-12T00:00:00Z/.. OR ../2018-02-12T00:00:00Z OR 2018-02-12T00:00:00Z/2018-03-18T12:31:12Z'),
    'limit': fields.Integer(required=False, example=10, default=10),
    'offset': fields.Integer(required=False, example=0, default=0),
})


@api.route('', methods=["get", "post"], strict_slashes=False)
@api.route('/', methods=["get", "post"], strict_slashes=False)
class IngestParquet(Resource):
    def __init__(self, api=None, *args, **kwargs):
        super().__init__(api, args, kwargs)

    def __get_time_range(self, cumulus: GranulesQuery, input_datetime: str):
        if '/' not in input_datetime:
            cumulus.with_time(input_datetime)
            return
        split_time_range = [k.strip() for k in input_datetime.split('/')]
        if split_time_range[0] == '..':
            cumulus.with_time_to(split_time_range[1])
            return
        if split_time_range[1] == '..':
            cumulus.with_time_from(split_time_range[0])
            return
        cumulus.with_time_range(split_time_range[0], split_time_range[1])
        return

    @api.expect()
    @authenticator_decorator
    def get(self, **kwargs):
        cumulus_base = os.getenv('CUMULUS_BASE')
        cumulus_token = kwargs['auth_jwt_token']
        cumulus = GranulesQuery(cumulus_base, cumulus_token)
        if 'limit' in request.args:
            cumulus.with_limit(int(request.args.get('limit')))
        if 'offset' in request.args:  # TODO might need to compute to get page_number
            cumulus.with_page_number(int(request.args.get('offset')))
        if 'datetime' in request.args:
            input_datetime = request.args.get('datetime')
            self.__get_time_range(cumulus, input_datetime)
        cumulus_result = cumulus.query()
        return {'features': cumulus_result}, 200
