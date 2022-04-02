# -*- coding: utf-8 -*-
"""
Created on Sat Apr 2 2022

@author: Michael Lin
"""
import sys
import logging
import pandas as pd

from abc import ABC, abstractmethod
from db_service import RedshiftConnection, SqlalchemyConnection
from utils import get_aws_data, get_api_data, get_db_data, get_csv_data

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


class ExtractorFactory(ABC):
    """
    Extract abstract factory
    This interface is used to expand into a family of products defined below where depends on different methodology of
    extractor is efined as a different concrete factory
    """
    def __init__(self, entity, date, file_name=None, file_path=None):
        self._entity = entity
        self._date = date
        self._file_name = file_name
        self._file_path = file_path

    @abstractmethod
    def create_extractor(self):
        pass


class AwsExtractorFactory(ExtractorFactory):
    """
    Aws extractor concrete factory
    This factory produces a family of products related to Aws
    """
    def __init__(self, entity, date, file_name, file_path):
        super(AwsExtractorFactory, self).__init__(entity, date, file_name, file_path)

    def create_extractor(self):
        return AwsDataExtractor(self._entity, self._date, self._file_name, self._file_path)


class DbExtractorFactory(ExtractorFactory):
    """
    Db extractor concrete factory
    This factory produces a family of products related to Db
    """
    def __init__(self, entity, date, dsn, file_name, file_path):
        super(DbExtractorFactory, self).__init__(entity, date, file_name, file_path)
        self._dsn = dsn

    def create_extractor(self):
        return DbDataExtractor(self._entity, self._date, self._dsn, self._file_name, self._file_path)


class ApiExtractorFactory(ExtractorFactory):
    """
    Api extractor concrete factory
    This factory produces a family of products related to API
    """
    def __init__(self, entity, date, file_name, file_path):
        super(ApiExtractorFactory, self).__init__(entity, date, file_name, file_path)

    def create_extractor(self):
        return ApiDataExtractor(self._entity, self._date, self._file_name, self._file_path)


class CsvExtractorFactory(ExtractorFactory):
    """
    Csv extractor concrete factory
    This factory produces a family of products related to CSV
    """
    def __init__(self, entity, date, file_name, file_path):
        super(CsvExtractorFactory, self).__init__(entity, date, file_name, file_path)

    def create_extractor(self):
        return CsvDataExtractor(self._entity, self._date, self._file_name, self._file_path)


class DataExtractor(ABC):
    """ Data extractor base interface with each variant implementing it differently """
    def __init__(self, entity, date, file_name, file_path):
        self._entity = entity
        self._date = date
        self._file_name = file_name
        self._file_path = file_path

    @abstractmethod
    def extract_data(self) -> pd.DataFrame():
        pass


class AwsDataExtractor(DataExtractor):
    """ Aws data extractor """
    def __init__(self, entity, date, file_name, file_path):
        super(AwsDataExtractor, self).__init__(entity, date, file_name, file_path)
        self._aws_conn = RedshiftConnection()

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through AWS for {self._entity} on {self._date}')
        sql = 'select top 10 * from random_table'  # This needs to be replaced
        try:
            return get_aws_data(sql=sql, conn=self._aws_conn, file_name=self._file_name, file_path=self._file_path)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through AWS')
            raise e


class DbDataExtractor(DataExtractor):
    """ Db data extractor """
    def __init__(self, entity, date, dsn, file_name, file_path):
        super(DbDataExtractor, self).__init__(entity, date, file_name, file_path)
        self._dsn = dsn
        self._engine = SqlalchemyConnection(dsn=self._dsn, app_name=__name__).setup()

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through DB for {self._entity} on {self._date}')
        sql = 'select top 10 * from random_table'  # This needs to be replaced
        try:
            return get_db_data(sql=sql, conn=self._engine, file_name=self._file_name, file_path=self._file_path)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through DB')
            raise e


class ApiDataExtractor(DataExtractor):
    """ Api data extractor """
    def __init__(self, entity, date, file_name, file_path):
        super(ApiDataExtractor, self).__init__(entity, date, file_name, file_path)

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through API for {self._entity} on {self._date}')
        url = 'https://random_website/data/index'  # This needs to be replaced
        try:
            return get_api_data(url=url, file_name=self._file_name, file_path=self._file_path)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through API')
            raise e


class CsvDataExtractor(DataExtractor):
    """ Csv data extractor """
    def __init__(self, entity, date, file_name, file_path):
        super(CsvDataExtractor, self).__init__(entity, date, file_name, file_path)

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through CSV for {self._entity} on {self._date}')
        csv_file_path = '/filepath/'  # This needs to be replaced
        try:
            return get_csv_data(file_path=csv_file_path)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through CSV')
            raise e


def run_extractor(factory: ExtractorFactory) -> pd.DataFrame():
    """ This function calls the abstract factory which instantiates the appropriate concrete factory """
    return factory.create_extractor()
