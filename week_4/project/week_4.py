from typing import List

from dagster import Nothing, asset, with_resources, In, Out
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])}
)
def get_s3_data(context):
    key_name = context.op_config["s3_key"]
    output = list()
    data = context.resources.s3.get_data(key_name)
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    ins={"stocks": In(dagster_type=List[Stock], description="List of Stocks")},
    out={"aggregation": Out(dagster_type=Aggregation, description="Aggregation of stock data")},
    group_name="corise"
)
def process_data(get_s3_data):
    stock_high = get_s3_data[0]
    for stock in get_s3_data[1:]:
        if stock.high > stock_high.high:
            stock_high = stock
    aggregation = Aggregation(date=stock_high.date, high=stock_high.high)
    return aggregation


@asset(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation, description="Aggregation of stock data")},
    group_name="corise"
)
def put_redis_data(context, process_data):
    date_str = process_data.date.isoformat()
    high_str = str(process_data.high)
    context.resources.redis.put_data(name=date_str, value=high_str)



get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources()
