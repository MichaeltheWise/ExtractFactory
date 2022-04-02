# -*- coding: utf-8 -*-
"""
Created on Sat Apr 2 2022

@author: Michael Lin
"""
import pandas as pd

from extractor import AwsExtractorFactory, ApiExtractorFactory, DbExtractorFactory, CsvExtractorFactory, run_extractor


class DataExtractor:
    def __init__(self, source, entity, date, dsn, file_name=None, file_path=None):
        self._source = source
        self._entity = entity
        self._date = date
        self._dsn = dsn
        self._file_name = file_name
        self._file_path = file_path

    def run(self):
        df = pd.DataFrame()
        match self._source:
            case 'AWS':
                df = run_extractor(AwsExtractorFactory(entity=self._entity, date=self._date, file_name=self._file_name,
                                                       file_path=self._file_path)).extract_data()
            case 'DB':
                df = run_extractor(
                    DbExtractorFactory(entity=self._entity, date=self._date, dsn=self._dsn, file_name=self._file_name,
                                       file_path=self._file_path)).extract_data()
            case 'API':
                df = run_extractor(ApiExtractorFactory(entity=self._entity, date=self._date, file_name=self._file_name,
                                                       file_path=self._file_path)).extract_data()
            case 'CSV':
                df = run_extractor(CsvExtractorFactory(entity=self._entity, date=self._date, file_name=self._file_name,
                                                       file_path=self._file_path)).extract_data()
        return df


if __name__ == '__main__':
    # Swap between sources
    test_data_extractor = DataExtractor(source='AWS', entity='GLOBAL_TRADING_DESK', date='2022-04-02', dsn=None)
    test_data_extractor.run()
