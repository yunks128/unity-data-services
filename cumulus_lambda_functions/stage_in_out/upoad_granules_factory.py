class UploadGranulesFactory:
    UPLOAD_S3_BY_STAC_CATALOG = 'UPLOAD_S3_BY_STAC_CATALOG'

    def get_class(self, upload_type):
        if upload_type == UploadGranulesFactory.UPLOAD_S3_BY_STAC_CATALOG:
            from cumulus_lambda_functions.stage_in_out.upload_granules_by_complete_catalog_s3 import UploadGranulesByCompleteCatalogS3
            return UploadGranulesByCompleteCatalogS3()
        raise ValueError(f'unknown search_type: {upload_type}')
