import logging
import os
from sys import argv

from cumulus_lambda_functions.cumulus_download_granules.download_granules import DownloadGranules
from cumulus_lambda_functions.cumulus_upload_granules.upload_granules import UploadGranules


def choose_process():
    if argv[1].strip().upper() == 'DOWNLOAD':
        logging.info('starting DOWNLOAD script')
        DownloadGranules().start()
    elif argv[1].strip().upper() == 'UPLOAD':
        logging.info('starting UPLOAD script')
        logging.info(UploadGranules().start())
    else:
        raise ValueError(f'invalid argument: {argv}')
    return


if __name__ == '__main__':
    logging.basicConfig(level=int(os.environ.get('LOG_LEVEL', '10')),
                        format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
    choose_process()
