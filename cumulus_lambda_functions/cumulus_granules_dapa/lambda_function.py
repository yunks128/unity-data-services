from cumulus_lambda_functions.cumulus_granules_dapa.cumulus_granules_dapa import CumulusGranulesDapa
from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator


def lambda_handler(event, context):
    """
{'cma': {'task_config': {'bucket': '{$.meta.buckets.internal.name}', 'collection': '{$.meta.collection}', 'cumulus_message': {'outputs': [{'source': '{$.files}', 'destination': '{$.payload}'}]}}, 'event': {'cumulus_meta': {'cumulus_version': '10.0.1', 'execution_name': 'c6d885dc-b4b2-4eb0-b22e-b6f58a7a0870', 'message_source': 'sfn', 'queueExecutionLimits': {'https://sqs.us-west-2.amazonaws.com/884500545225/am-uds-dev-cumulus-backgroundProcessing': 5}, 'state_machine': 'arn:aws:states:us-west-2:884500545225:stateMachine:am-uds-dev-cumulus-IngestGranule', 'system_bucket': 'am-uds-dev-cumulus-internal', 'workflow_start_time': 1646785175509, 'parentExecutionArn': 'arn:aws:states:us-west-2:884500545225:execution:am-uds-dev-cumulus-DiscoverGranules:885483b4-ba55-4db1-b197-661e1e595a45', 'queueUrl': 'arn:aws:sqs:us-west-2:884500545225:am-uds-dev-cumulus-startSF'}, 'exception': 'None', 'meta': {'buckets': {'internal': {'name': 'am-uds-dev-cumulus-internal', 'type': 'internal'}, 'protected': {'name': 'am-uds-dev-cumulus-protected', 'type': 'protected'}}, 'cmr': {'clientId': 'CHANGEME', 'cmrEnvironment': 'UAT', 'cmrLimit': 100, 'cmrPageSize': 50, 'oauthProvider': 'earthdata', 'passwordSecretName': 'am-uds-dev-cumulus-message-template-cmr-password20220216072916956000000002', 'provider': 'CHANGEME', 'username': 'username'}, 'collection': {'name': 'ATMS_SCIENCE_Group_2011', 'version': '001', 'process': 'modis', 'granuleId': '^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$', 'granuleIdExtraction': '(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+', 'sampleFileName': 'P1570515ATMSSCIENCEAXT11344000000001.PDS', 'duplicateHandling': 'replace', 'url_path': '{cmrMetadata.Granule.Collection.ShortName}___{cmrMetadata.Granule.Collection.VersionId}', 'provider_path': '/data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/', 'files': [{'bucket': 'internal', 'regex': '^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$', 'sampleFileName': 'P1570515ATMSSCIENCEAXT11344000000000.PDS', 'type': 'data'}, {'bucket': 'internal', 'regex': '^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS$', 'sampleFileName': 'P1570515ATMSSCIENCEAXT11344000000001.PDS', 'type': 'metadata'}, {'bucket': 'internal', 'regex': '^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$', 'sampleFileName': 'P1570515ATMSSCIENCEAXT11344000000001.PDS.xml', 'type': 'metadata'}, {'bucket': 'internal', 'regex': '^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0\\.cmr\\.xml$', 'sampleFileName': 'P1570515ATMSSCIENCEAXT11344000000001.PDS.xml', 'type': 'metadata'}], 'updatedAt': 1646326197526, 'createdAt': 1646258167624}, 'distribution_endpoint': 's3://am-uds-dev-cumulus-internal/', 'launchpad': {'api': 'launchpadApi', 'certificate': 'launchpad.pfx', 'passphraseSecretName': ''}, 'provider': {'password': 'AQICAHhSagsGDAl5tQWM010IEvxKgj2LcsNub5v5FHoRpOjXcQHFbE4iMnF/W0Y/NrsYvrfHAAAAajBoBgkqhkiG9w0BBwagWzBZAgEAMFQGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQMLaH13SdxPXREjXLtAgEQgCfA+lEu2c/xLTGwJsbtKlXJbKDy4pwV+rS3BnJqgBoLLMQZqOdoFhk=', 'host': 'snppl0.gesdisc.eosdis.nasa.gov', 'updatedAt': 1646244053419, 'protocol': 'https', 'createdAt': 1646244053419, 'encrypted': True, 'username': 'AQICAHhSagsGDAl5tQWM010IEvxKgj2LcsNub5v5FHoRpOjXcQGRoY5EBMpvvyMASUowBM61AAAAYzBhBgkqhkiG9w0BBwagVDBSAgEAME0GCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQM9OhRHwTuxiz74q4UAgEQgCDEHOhsVG6+LqXfnlw+Z3Wg9MDOCd9/K5/X5j3tPJYkaA==', 'allowedRedirects': ['https://urs.earthdata.nasa.gov', 'urs.earthdata.nasa.gov'], 'id': 'snpp_provider_02', 'globalConnectionLimit': 10}, 'stack': 'am-uds-dev-cumulus', 'template': 's3://am-uds-dev-cumulus-internal/am-uds-dev-cumulus/workflow_template.json', 'workflow_name': 'IngestGranule', 'workflow_tasks': {'SyncGranule': {'name': 'am-uds-dev-cumulus-SyncGranule', 'version': '$LATEST', 'arn': 'arn:aws:lambda:us-west-2:884500545225:function:am-uds-dev-cumulus-SyncGranule'}}, 'staticValue': 'aStaticValue', 'interpolatedValueStackName': 'am-uds-dev-cumulus', 'input_granules': [{'granuleId': 'P1570515ATMSSCIENCEAXT1134912000000', 'dataType': 'ATMS_SCIENCE_Group_2011', 'version': '001', 'files': [{'bucket': 'am-uds-dev-cumulus-internal', 'key': 'file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000000.PDS', 'source': 'data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/349//P1570515ATMSSCIENCEAXT11349120000000.PDS', 'fileName': 'P1570515ATMSSCIENCEAXT11349120000000.PDS', 'type': 'data', 'size': 744}, {'bucket': 'am-uds-dev-cumulus-internal', 'key': 'file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000001.PDS', 'source': 'data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/349//P1570515ATMSSCIENCEAXT11349120000001.PDS', 'fileName': 'P1570515ATMSSCIENCEAXT11349120000001.PDS', 'type': 'metadata', 'size': 18084600}, {'bucket': 'am-uds-dev-cumulus-internal', 'key': 'file-staging/am-uds-dev-cumulus/ATMS_SCIENCE_Group_2011___001/P1570515ATMSSCIENCEAXT11349120000001.PDS.xml', 'source': 'data/SNPP_ATMS_Level0_T/ATMS_SCIENCE_Group/2011/349//P1570515ATMSSCIENCEAXT11349120000001.PDS.xml', 'fileName': 'P1570515ATMSSCIENCEAXT11349120000001.PDS.xml', 'type': 'metadata', 'size': 9526}], 'sync_granule_duration': 9822, 'createdAt': 1647386972717}], 'process': 'modis'}, 'payload': {}, 'replace': {'Bucket': 'am-uds-dev-cumulus-internal', 'Key': 'events/5d8edf37-0a18-4af5-a76f-7c2091cdd1e2', 'TargetPath': '$.payload'}}}}
    :param event:
    :param context:
    :return:
    """
    LambdaLoggerGenerator.remove_default_handlers()
    # TODO implement
    return CumulusGranulesDapa(event).start()
