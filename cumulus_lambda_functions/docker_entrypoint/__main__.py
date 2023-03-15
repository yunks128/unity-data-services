import logging
import os
from sys import argv

from cumulus_lambda_functions.stage_in_out.catalog_granules_factory import CatalogGranulesFactory
from cumulus_lambda_functions.stage_in_out.download_granules_s3 import DownloadGranulesS3
from cumulus_lambda_functions.stage_in_out.search_granules_factory import SearchGranulesFactory
from cumulus_lambda_functions.stage_in_out.upoad_granules_factory import UploadGranulesFactory


def choose_process():
    if argv[1].strip().upper() == 'SEARCH':
        logging.info('starting SEARCH script')
        return SearchGranulesFactory().get_class(os.getenv('GRANULES_SEARCH_DOMAIN', 'MISSING_GRANULES_SEARCH_DOMAIN')).search()
    if argv[1].strip().upper() == 'DOWNLOAD':
        logging.info('starting DOWNLOAD script')
        return DownloadGranulesS3().download()
    if argv[1].strip().upper() == 'UPLOAD':
        logging.info('starting UPLOAD script')
        return UploadGranulesFactory().get_class(os.getenv('GRANULES_UPLOAD_TYPE', 'MISSING_GRANULES_UPLOAD_TYPE')).upload()
    if argv[1].strip().upper() == 'CATALOG':
        logging.info('starting CATALOG script')
        return CatalogGranulesFactory().get_class(os.getenv('GRANULES_CATALOG_TYPE', 'MISSING_GRANULES_CATALOG_TYPE')).catalog()
    raise ValueError(f'invalid argument: {argv}')


if __name__ == '__main__':
    logging.basicConfig(level=int(os.environ.get('LOG_LEVEL', '10')),
                        format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
    print(choose_process())
