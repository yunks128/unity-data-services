"""
Lambda workflow:
Lambda will check if all files are available in S3.
If not, it will send back to SQS with assumption that they will be availalbe soon although it should not happen 99% of the time.
Lambda will retrieve the Collection ID from stac metadata file.
Lambda will assume that all files from the same catalog.json belongs to the same collection. and won't validate it.
Lambda will check if collection exists and will attempt to create one if it does not exist.
Information needed for a collection is listed below.
This needs further discussion.
If collection creation fails, it will send the message back to SQS and notifies admin via SNS.
Lambda will submit cnm request via SNS.

"""