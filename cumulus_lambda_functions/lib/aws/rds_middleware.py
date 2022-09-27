from sqlalchemy import create_engine
from sqlalchemy import text

from cumulus_lambda_functions.lib.lambda_logger_generator import LambdaLoggerGenerator

LOGGER = LambdaLoggerGenerator.get_logger(__name__, LambdaLoggerGenerator.get_level_from_env())


class RdsMiddleware:
    def __init__(self, rds_domain: str, db_name: str, username: str, password: str, port: int=5432):
        self.__rds_domain = rds_domain
        self.__db_name = db_name
        self.__username = username
        self.__password = password
        self.__port = port

    def query(self, select_stmt: str):
        db_str = f'postgresql://{self.__username}:{self.__password}@{self.__rds_domain}:{self.__port}/{self.__db_name}'
        LOGGER.debug(f'db_str: {db_str.replace(self.__password, "xxx")}')
        LOGGER.debug(f'select_stmt: {select_stmt}')
        engine = create_engine(db_str)
        with engine.connect() as connection:
            result = connection.execute(text(select_stmt))
            result = [k for k in result]
        return result
