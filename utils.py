# -*- coding: utf-8 -*-
"""
Created on Sat Apr 2 2022

@author: Michael Lin
"""
import requests
import os
import time
import datetime as dt
import pandas as pd

from decorators import cache


@cache
def get_aws_data(sql, conn, file_name, file_path):
    """
    Extract data from AWS
    :param sql: sql
    :param conn: Redshift Connection
    :param file_name: file_name
    :param file_path: file_path
    :return: dataframe
    """
    with conn.connection as aws_conn:
        df = pd.read_sql_query(sql, aws_conn)
    output_path = dt.datetime.now().strftime('%Y-%m-%d') + '_' + file_name + '.csv'
    df.to_csv(os.path.join(file_path, output_path))
    return df


@cache
def get_api_data(url, file_name, file_path):
    """
    Extract data from API
    :param url: url
    :param file_name: file_name
    :param file_path: file_path
    :return: dataframe
    """
    json_resp = make_request_with_retries(url)
    if not json_resp:
        raise ValueError('No data found')
    df = pd.DataFrame(json_resp['data'])
    output_path = dt.datetime.now().strftime('%Y-%m-%d') + '_' + file_name + '.csv'
    df.to_csv(os.path.join(file_path, output_path))
    return df


@cache
def get_db_data(conn, sql, file_name, file_path):
    """
    Extract data from database using sqlalchemy
    :param conn: Sqlalchemy Connection
    :param sql: sql
    :param file_name: file_name
    :param file_path: file_path
    :return: dataframe
    """
    df = pd.read_sql_query(sql=sql, con=conn)
    output_path = dt.datetime.now().strftime('%Y-%m-%d') + '_' + file_name + '.csv'
    df.to_csv(os.path.join(file_path, output_path))
    return df


def get_csv_data(file_path):
    """
    Extract data from csv
    :param file_path: file_path
    :return: dataframe
    """
    df = pd.read_csv(file_path, index_col=0)
    return df


def make_request_with_retries(endpoint, headers=None, verify=False, retries=5, sleep_time=60):
    """ Make request with retries """
    json_response = None
    print(f'Making get request for endpoint {endpoint}')
    while retries >= 0:
        try:
            resp = requests.get(endpoint, headers=headers, verify=verify)
            resp.raise_for_status()
            json_response = resp.json()
            break
        except (ValueError, requests.exceptions.HTTPError) as e:
            if retries != 0:
                retries -= 1
                time.sleep(sleep_time)
                print(f'Retrying {endpoint}')
                continue
            else:
                print(f"Endpoint request failed {retries} consecutive times with exception: {str(e)}")
                raise e

    return json_response
