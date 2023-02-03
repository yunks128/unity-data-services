
from cumulus_lambda_functions.stage_in_out.upload_granules_abstract import UploadGranulesAbstract


class UploadGranulesS3(UploadGranulesAbstract):
    COLLECTION_ID_KEY = 'COLLECTION_ID'
    PROVIDER_ID_KEY = 'PROVIDER_ID'
    UPLOAD_DIR_KEY = 'UPLOAD_DIR'
    STAGING_BUCKET_KEY = 'STAGING_BUCKET'

    VERIFY_SSL_KEY = 'VERIFY_SSL'
    DELETE_FILES_KEY = 'DELETE_FILES'

    def __init__(self) -> None:
        super().__init__()

    def upload(self, **kwargs) -> list:
        raise NotImplementedError()
