

class UploadGranulesFactory:
    S3 = 'S3'

    def get_class(self, upload_type):
        if upload_type == UploadGranulesFactory.S3:
            from cumulus_lambda_functions.stage_in_out.upload_granules_s3 import UploadGranulesS3
            return UploadGranulesS3()
        raise ValueError(f'unknown search_type: {upload_type}')
