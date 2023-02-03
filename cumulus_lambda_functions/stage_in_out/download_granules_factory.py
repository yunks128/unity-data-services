class DownloadGranulesFactory:
    S3 = 'S3'

    def get_class(self, search_type):
        if search_type == DownloadGranulesFactory.S3:
            from cumulus_lambda_functions.stage_in_out.download_granules_s3 import DownloadGranulesS3
            return DownloadGranulesS3()
        raise ValueError(f'unknown search_type: {search_type}')
