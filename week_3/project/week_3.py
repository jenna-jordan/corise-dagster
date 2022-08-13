from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
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
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"])
def docker_config(partition_key: str):
    return {
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
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)

# run the local_week_3_pipeline every 15 minutes
local_week_3_schedule = ScheduleDefinition(
    job=local_week_3_pipeline, 
    cron_schedule= "*/15 * * * *")  

# run the docker_week_3_pipeline at the start of every hour
docker_week_3_schedule = ScheduleDefinition(
    job=docker_week_3_pipeline, 
    cron_schedule= "0 * * * *")    


@sensor(job=docker_week_3_pipeline)
def docker_week_3_sensor(context):
    new_s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://host.docker.internal:4566")
    if not new_s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for key in new_s3_keys:
        yield RunRequest(
            run_key=key,
            run_config={"ops": {"get_s3_data": {"config": {"s3_key": key}}}}
        )
