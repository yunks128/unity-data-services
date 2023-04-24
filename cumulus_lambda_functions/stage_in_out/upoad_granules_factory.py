class UploadGranulesFactory:
    S3 = 'S3'
    CATALOG_S3 = 'CATALOG_S3'

    def get_class(self, upload_type):
        if upload_type == UploadGranulesFactory.S3:
            from cumulus_lambda_functions.stage_in_out.upload_granules_s3 import UploadGranulesS3
            return UploadGranulesS3()
        if upload_type == UploadGranulesFactory.CATALOG_S3:
            from cumulus_lambda_functions.stage_in_out.upload_granules_by_catalog_s3 import UploadGranulesByCatalogS3
            return UploadGranulesByCatalogS3()
        raise ValueError(f'unknown search_type: {upload_type}')
