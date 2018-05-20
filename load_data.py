"""loda_data.py

This script will load some data into Amazon DynamoDB.
Currently there are 3 types of data.

* Sparse data
  This data tries to emulate alert data that IoT devices produce.
  It accepts several parameters as follows:
  * rate: How often the device will produce alert signals

* Metrics data
  This creates continuous data to immitate metrics data.
"""

import boto3
import datetime
import dateutil
import decimal
import random
import tqdm
import uuid

from dateutil.relativedelta import relativedelta

decimal_context = decimal.getcontext()
decimal_context.prec = 3

def generate_sparse_data(start_dt, duration, interval_mean, interval_var, sid='ABC-1234567'):
    """Randomly generate alert data

    start_dt: `datetime.datetime`. Inclusive
    duration: `dateutil.relativedelta.relativedelta`
    interval_mean: An average of interval (seconds) between data generatio
    interval_var: same as above"""
    current_dt = start_dt
    while current_dt <= start_dt + duration:
        interval = abs(random.gauss(interval_mean, interval_var))
        current_dt += relativedelta(seconds=interval)
        yield { 'sid': sid,
                'unixtime': int(current_dt.timestamp()),
                'realunixtime': decimal.Decimal(current_dt.timestamp()),
                'somedata': uuid.uuid1().hex }

def generate_continuous_data(start_dt:datetime.datetime, 
                             duration:dateutil.relativedelta.relativedelta,
                             interval_sec: int,
                             sid='ABC-1234567',
                             data_len=3,
                             aggr=False):
    """Generate continous values
    
    aggr: If specified, this method assumes each data  aggregated values"""
    assert(interval_sec >= 1)

    current_dt = start_dt
    while current_dt <= start_dt + duration:
        current_dt += relativedelta(seconds=interval_sec)
        if aggr:
            data = [uuid.uuid1().hex[:data_len] for i in range(interval_sec)]
        else:
            data = uuid.uuid1().hex[:data_len]

        yield { 'sid': sid,
                'unixtime': int(current_dt.timestamp()),
                'realunixtime': decimal.Decimal(current_dt.timestamp()),
                'somedata': data }

def create_table(dynamo_client, table_name, rcu=5, wcu=5):
    try:
        dynamo_client.delete_table(TableName=table_name)
        with dynamo_client.get_waiter('table_not_exists') as waiter:
            waiter.wait(TableName=table_name)
    except Exception as e:
        print(e)
    dynamo_client.create_table(
            TableName='sparse-data',
            KeySchema=[{
                'AttributeName': 'sid',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'unixtime',
                'KeyType': 'RANGE'
            }],
            AttributeDefinitions=[{
                'AttributeName': 'sid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'unixtime',
                'AttributeType': 'N'
            }],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
    )
    with dynamo_client.get_waiter('table_exists') as waiter:
        waiter.wait(TableName=table_name)

def create_tables(dynamo_client):
    create_table(dynamo_client, 'sparse-data')
    create_table(dynamo_client, 'continuous-data', rcu=5, wcu=15)
    create_table(dynamo_client, 'continuous-data-aggr')
    
def main():
    dynamo = boto3.resource('dynamodb')
    create_tables(boto3.client('dynamodb'))

    start_dt = datetime.datetime.now()
    duration = relativedelta(years=5)  # 5年

    ## generate & import data into sparse-data table
    table_name = 'sparse-data'
    pbar = tqdm.tqdm(total=5*365*24*10)
    sparse_table = dynamo.Table(table_name)
    with sparse_table.batch_writer() as batch:
        count = 0
        for data in generate_sparse_data(start_dt, duration, 600, 120):
            batch.put_item(Item=data)
            count += 1
            pbar.update(count)

    ## generate & import data into continous-data table
    table_name = 'continuous-data'
    pbar = tqdm.tqdm(total=5*365*24*3600)
    continous_table = dynamo.Table(table_name)
    with continous_table.batch_writer() as batch:
        count = 0
        for data in generate_continuous_data(start_dt, duration, 1):
            batch.put_item(Item=data)
            count += 1
            pbar.update(count)

    table_name = 'continous-data-aggr'
    pbar = tqdm.tqdm(total=5*365*24*20)
    continous_aggr_table = dynamo.Table(table_name)
    with continous_aggr_table.batch_writer() as batch:
        count = 0
        for data in generate_continuous_data(start_dt, duration, 180, aggr=True):
            batch.put_item(Item=data)
            count += 1
            pbar.update(count)

if __name__ == '__main__':
    main()
