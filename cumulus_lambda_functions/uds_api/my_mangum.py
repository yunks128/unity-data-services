import logging
import os

from cumulus_lambda_functions.uds_api.web_service_constants import WebServiceConstants
from mangum import Mangum
from mangum.types import (
    LambdaEvent,
    LambdaContext,
)
logger = logging.getLogger("mangum")


class MyMangum(Mangum):

    def __call__(self, event: LambdaEvent, context: LambdaContext) -> dict:
        """
        Overriding the original Mangum class so that we can store the deployed stage somewhere.
        Currently, it is stored in ENV variable.

        :param event:
        :param context:
        :return:
        """
        os.environ[WebServiceConstants.DEPLOYED_STAGE] = event['requestContext']['stage']
        return super().__call__(event, context)

'''
Sample input Lambda Event
{
  "resource": "/sbx-uds-dapa/collections",
  "path": "/sbx-uds-dapa/collections/",
  "httpMethod": "GET",
  "headers": {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Authorization": "Bearer <token>",
    "CloudFront-Forwarded-Proto": "https",
    "CloudFront-Is-Android-Viewer": "false",
    "CloudFront-Is-Desktop-Viewer": "true",
    "CloudFront-Is-IOS-Viewer": "false",
    "CloudFront-Is-Mobile-Viewer": "false",
    "CloudFront-Is-SmartTV-Viewer": "false",
    "CloudFront-Is-Tablet-Viewer": "false",
    "CloudFront-Viewer-Address": "128.149.240.221:58454",
    "CloudFront-Viewer-ASN": "127",
    "CloudFront-Viewer-City": "Altadena",
    "CloudFront-Viewer-Country": "US",
    "CloudFront-Viewer-Country-Name": "United States",
    "CloudFront-Viewer-Country-Region": "CA",
    "CloudFront-Viewer-Country-Region-Name": "California",
    "CloudFront-Viewer-HTTP-Version": "1.1",
    "CloudFront-Viewer-Latitude": "34.19310",
    "CloudFront-Viewer-Longitude": "-118.13830",
    "CloudFront-Viewer-Metro-Code": "803",
    "CloudFront-Viewer-Postal-Code": "91001",
    "CloudFront-Viewer-Time-Zone": "America/Los_Angeles",
    "CloudFront-Viewer-TLS": "TLSv1.3:TLS_AES_128_GCM_SHA256:fullHandshake",
    "Host": "1gp9st60gd.execute-api.us-west-2.amazonaws.com",
    "User-Agent": "python-requests/2.31.0",
    "Via": "1.1 d118b2ea8414d381f46f91331ab67f02.cloudfront.net (CloudFront)",
    "X-Amz-Cf-Id": "9IHTypkwvvJavXvF86CTJW5vDlVaEvwiReuXAvsYEbGl2_Ka-bTqBQ==",
    "X-Amzn-Trace-Id": "Root=1-654aeb8e-5a6ecea56e8536646971cf48",
    "X-Forwarded-For": "128.149.240.221, 18.68.45.10",
    "X-Forwarded-Port": "443",
    "X-Forwarded-Proto": "https"
  },
  "multiValueHeaders": {
    "Accept": [
      "*/*"
    ],
    "Accept-Encoding": [
      "gzip, deflate"
    ],
    "Authorization": [
      "Bearer <token>"
    ],
    "CloudFront-Forwarded-Proto": [
      "https"
    ],
    "CloudFront-Is-Android-Viewer": [
      "false"
    ],
    "CloudFront-Is-Desktop-Viewer": [
      "true"
    ],
    "CloudFront-Is-IOS-Viewer": [
      "false"
    ],
    "CloudFront-Is-Mobile-Viewer": [
      "false"
    ],
    "CloudFront-Is-SmartTV-Viewer": [
      "false"
    ],
    "CloudFront-Is-Tablet-Viewer": [
      "false"
    ],
    "CloudFront-Viewer-Address": [
      "128.149.240.221:58454"
    ],
    "CloudFront-Viewer-ASN": [
      "127"
    ],
    "CloudFront-Viewer-City": [
      "Altadena"
    ],
    "CloudFront-Viewer-Country": [
      "US"
    ],
    "CloudFront-Viewer-Country-Name": [
      "United States"
    ],
    "CloudFront-Viewer-Country-Region": [
      "CA"
    ],
    "CloudFront-Viewer-Country-Region-Name": [
      "California"
    ],
    "CloudFront-Viewer-HTTP-Version": [
      "1.1"
    ],
    "CloudFront-Viewer-Latitude": [
      "34.19310"
    ],
    "CloudFront-Viewer-Longitude": [
      "-118.13830"
    ],
    "CloudFront-Viewer-Metro-Code": [
      "803"
    ],
    "CloudFront-Viewer-Postal-Code": [
      "91001"
    ],
    "CloudFront-Viewer-Time-Zone": [
      "America/Los_Angeles"
    ],
    "CloudFront-Viewer-TLS": [
      "TLSv1.3:TLS_AES_128_GCM_SHA256:fullHandshake"
    ],
    "Host": [
      "1gp9st60gd.execute-api.us-west-2.amazonaws.com"
    ],
    "User-Agent": [
      "python-requests/2.31.0"
    ],
    "Via": [
      "1.1 d118b2ea8414d381f46f91331ab67f02.cloudfront.net (CloudFront)"
    ],
    "X-Amz-Cf-Id": [
      "9IHTypkwvvJavXvF86CTJW5vDlVaEvwiReuXAvsYEbGl2_Ka-bTqBQ=="
    ],
    "X-Amzn-Trace-Id": [
      "Root=1-654aeb8e-5a6ecea56e8536646971cf48"
    ],
    "X-Forwarded-For": [
      "128.149.240.221, 18.68.45.10"
    ],
    "X-Forwarded-Port": [
      "443"
    ],
    "X-Forwarded-Proto": [
      "https"
    ]
  },
  "queryStringParameters": {
    "limit": "100"
  },
  "multiValueQueryStringParameters": {
    "limit": [
      "100"
    ]
  },
  "pathParameters": null,
  "stageVariables": null,
  "requestContext": {
    "resourceId": "qwevpj",
    "authorizer": {
      "principalId": "user",
      "integrationLatency": 1374
    },
    "resourcePath": "/sbx-uds-dapa/collections",
    "httpMethod": "GET",
    "extendedRequestId": "ODm-TGTfPHcEJig=",
    "requestTime": "08/Nov/2023:01:59:42 +0000",
    "path": "/dev/sbx-uds-dapa/collections/",
    "accountId": "237868187491",
    "protocol": "HTTP/1.1",
    "stage": "dev",
    "domainPrefix": "1gp9st60gd",
    "requestTimeEpoch": 1699408782415,
    "requestId": "0a81ceb0-6785-48c3-ba34-dac86196fb73",
    "identity": {
      "cognitoIdentityPoolId": null,
      "accountId": null,
      "cognitoIdentityId": null,
      "caller": null,
      "sourceIp": "128.149.240.221",
      "principalOrgId": null,
      "accessKey": null,
      "cognitoAuthenticationType": null,
      "cognitoAuthenticationProvider": null,
      "userArn": null,
      "userAgent": "python-requests/2.31.0",
      "user": null
    },
    "domainName": "1gp9st60gd.execute-api.us-west-2.amazonaws.com",
    "apiId": "1gp9st60gd"
  },
  "body": null,
  "isBase64Encoded": false
}
'''