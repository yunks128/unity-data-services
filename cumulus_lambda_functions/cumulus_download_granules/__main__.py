import logging
import os

from cumulus_lambda_functions.cumulus_download_granules.download_granules import DownloadGranules

if __name__ == '__main__':
    logging.basicConfig(level=int(os.environ.get('LOG_LEVEL', '10')),
                        format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
    DownloadGranules().start()
