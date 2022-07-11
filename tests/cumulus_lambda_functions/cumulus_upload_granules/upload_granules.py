import os


os.environ['DAPA_API'] = 'https://k3a3qmarxh.execute-api.us-west-2.amazonaws.com/dev/am-uds-dapa'
os.environ['UNITY_BEARER_TOKEN'] = 'abcd.abcd.abcd-abcd-abcd'
os.environ['COLLECTION_ID'] = 'SNDR_SNPP_ATMS_L1A___1'
os.environ['PROVIDER_ID'] = 'SNPP'
os.environ['UPLOAD_DIR'] = '/tmp/snpp_upload_test_1'
os.environ['STAGING_BUCKET'] = 'am-uds-dev-cumulus-staging'
os.environ['VERIFY_SSL'] = 'false'
os.environ['DELETE_FILES'] = 'false'

from cumulus_lambda_functions.cumulus_upload_granules.upload_granules import UploadGranules
print(UploadGranules().start())