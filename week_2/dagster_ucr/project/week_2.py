from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    config_schema={"key_name": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])}
)
def get_s3_data(context):
    key_name = context.op_config["key_name"]
    output = list()
    data = context.resources.s3.get_data(key_name)
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output

@op(
    ins={"stocks": In(dagster_type=List[Stock], description="List of Stocks")},
    out={"aggregation": Out(dagster_type=Aggregation, description="Aggregation of stock data")},
)
def process_data(stocks: List[Stock]):
    stock_high = stocks[0]
    for stock in stocks[1:]:
        if stock.high > stock_high.high:
            stock_high = stock
    aggregation = Aggregation(date=stock_high.date, high=stock_high.high)
    return aggregation


@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation, description="Aggregation of stock data")}
)
def put_redis_data(context, aggregation: Aggregation):
    date_str = aggregation.date.isoformat()
    high_str = str(aggregation.high)
    context.resources.redis.put_data(name=date_str, value=high_str)


@graph
def week_2_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "preifx/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
