# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr
from pydantic.typing import Literal

from feast import Entity, FeatureView, utils
from feast.infra.infra_object import DYNAMODB_INFRA_OBJECT_CLASS_TYPE, InfraObject
from feast.infra.online_stores.helpers import compute_entity_id
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.DynamoDBTable_pb2 import (
    DynamoDBTable as DynamoDBTableProto,
)
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage, tracing_span

try:
    import amazondax
    import boto3
    import botocore
    from botocore.exceptions import ClientError
except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aws", str(e))


logger = logging.getLogger(__name__)


class DynamoDBOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for DynamoDB store"""

    type: Literal["dynamodb"] = "dynamodb"
    """Online store type selector"""

    region: StrictStr
    """AWS Region Name"""

    dax_cluster_endpoint: Optional[StrictStr] = None
    """(optional) DAX cluster endpoint"""

    iam_role: Optional[StrictStr] = None
    """(optional) IAM Role to assume, if needed e.g. for cross-account access"""

    boto3_read_timeout: Optional[int] = None
    """(optional) boto3 read timeout config"""

    boto3_max_pool_connections: Optional[int] = None
    """(optional) boto3 read timeout config"""


class DynamoDBOnlineStore(OnlineStore):
    """
    Online feature store for AWS DynamoDB.
    """

    _dynamodb_client = None

    @log_exceptions_and_usage(online_store="dynamodb")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_client = self._get_dynamodb_client(online_config)
        dynamodb_resource = self._get_dynamodb_resource(online_config)

        for table_instance in tables_to_keep:
            try:
                dynamodb_resource.create_table(
                    TableName=_get_table_name(config, table_instance),
                    KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
                    AttributeDefinitions=[
                        {"AttributeName": "entity_id", "AttributeType": "S"}
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
            except ClientError as ce:
                # If the table creation fails with ResourceInUseException,
                # it means the table already exists or is being created.
                # Otherwise, re-raise the exception
                if ce.response["Error"]["Code"] != "ResourceInUseException":
                    raise

        for table_instance in tables_to_keep:
            dynamodb_client.get_waiter("table_exists").wait(
                TableName=_get_table_name(config, table_instance)
            )

        for table_to_delete in tables_to_delete:
            _delete_table_idempotent(
                dynamodb_resource, _get_table_name(config, table_to_delete)
            )

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_resource = self._get_dynamodb_resource(online_config)

        for table in tables:
            _delete_table_idempotent(dynamodb_resource, _get_table_name(config, table))

    @log_exceptions_and_usage(online_store="dynamodb")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)

        dynamodb_resource = self._get_dynamodb_resource_for_writes(online_config)

        table_instance = dynamodb_resource.Table(_get_table_name(config, table))
        with table_instance.batch_writer() as batch:
            for entity_key, features, timestamp, created_ts in data:
                entity_id = compute_entity_id(entity_key)
                batch.put_item(
                    Item={
                        "entity_id": entity_id,  # PartitionKey
                        "event_ts": str(utils.make_tzaware(timestamp)),
                        "values": {
                            k: v.SerializeToString()
                            for k, v in features.items()  # Serialized Features
                        },
                    }
                )
                if progress:
                    progress(1)

    @log_exceptions_and_usage(online_store="dynamodb")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        online_config = config.online_store
        assert isinstance(online_config, DynamoDBOnlineStoreConfig)
        dynamodb_client = self._get_dynamodb_client(online_config)
        table_name = _get_table_name(config, table)
        entity_keys_str = [compute_entity_id(k) for k in entity_keys]

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        with tracing_span(name="remote_call"):
            responses = _do_single_table_batch_get(
                dynamodb_client, table_name, entity_keys_str
            )
        result_map_by_entity_id = {}
        for item in responses:
            values = item.get("values")

            if values is not None:
                res = {}
                for feature_name, value_bin in values["M"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin["B"])
                    res[feature_name] = val
                result_map_by_entity_id[item["entity_id"]["S"]] = (
                    datetime.fromisoformat(item["event_ts"]["S"]),
                    res,
                )
        result = []
        # return the correct ordering as implicitly expected by upstream
        for entity_key in entity_keys_str:
            if entity_key in result_map_by_entity_id:
                result.append(result_map_by_entity_id[entity_key])
            else:
                result.append((None, None))
        return result

    def _get_dynamodb_client(self, online_config: DynamoDBOnlineStoreConfig):
        if self._dynamodb_client is None:
            self._dynamodb_client = _initialize_dynamodb_client(online_config)
        return self._dynamodb_client

    def _get_dynamodb_resource(self, online_config: DynamoDBOnlineStoreConfig):
        threadlocal = threading.local()
        if "dynamodb_resource" not in threadlocal.__dict__:
            threadlocal.dynamodb_resource = _initialize_dynamodb_resource(online_config)
        return threadlocal.dynamodb_resource

    def _get_dynamodb_resource_for_writes(
        self, online_config: DynamoDBOnlineStoreConfig
    ):
        """Support DAX which uses amazondax and not boto3,
        but amazondax does not support table operations (so we still need _get_dynamodb_resource())

        We can eventually merge to use _get_dynamodb_client(), but the resource object has a nice batch_writer which retries batch_write on unprocessed items,
        though internally it does not support perform exponential backoff during flushing https://github.com/boto/boto3/blob/develop/boto3/dynamodb/table.py#L164.

        """
        threadlocal = threading.local()
        if "dynamodb_resource_for_writes" not in threadlocal.__dict__:
            threadlocal.dynamodb_resource_for_writes = _initialize_dynamodb_resource_for_writes(
                online_config
            )
        return threadlocal.dynamodb_resource_for_writes


def _do_single_table_batch_get(
    client, table_name, entity_keys, max_tries=10, base_backoff_time=0.01
):
    """
    Gets a batch of items from Amazon DynamoDB. Batches can contain keys from
    more than one table, but this function assume one table due to upstream limitation.

    When Amazon DynamoDB cannot process all items in a batch, a set of unprocessed
    keys is returned. This function uses an exponential backoff algorithm to retry
    getting the unprocessed keys until all are retrieved or the specified
    number of tries is reached.

    :param entity_keys: The set of keys to retrieve. A batch can contain at most 100
                        keys. Otherwise, Amazon DynamoDB returns an error.
    :param max_tries: max number of attempts for retries when DynamoDB returns UnprocessedKeys
    :param base_backoff_time: base of exponetial backoff time (in seconds), doubling every retry
    :return: The dictionary of retrieved items grouped under their respective
             table names.
    """
    tries = 0
    retrieved = []
    batch_keys = entity_keys.copy()
    while tries < max_tries:
        request = {table_name: {"Keys": [{"entity_id": {"S": k}} for k in batch_keys]}}
        response = client.batch_get_item(RequestItems=request)
        # Collect any retrieved items and retry unprocessed keys.
        retrieved = response["Responses"][table_name]
        unprocessed = response["UnprocessedKeys"]
        if len(unprocessed) > 0:
            batch_keys = unprocessed
            unprocessed_count = sum(
                [len(batch_key["Keys"]) for batch_key in batch_keys.values()]
            )
            logger.debug(
                "batch_get_item: %s unprocessed keys returned. Sleep, then retry.",
                unprocessed_count,
            )
            tries += 1
            if tries <= max_tries:
                time.sleep(base_backoff_time)
                base_backoff_time = min(base_backoff_time * 2, 8)
            else:
                # we could return to clients a incomplete results but they would expect to have all the items retrived,
                # failure could happen in batch get if one table is heavy-throttled
                # and failing would help them notice the problem and adjust capacity.
                raise RuntimeError(
                    "batch_get_item failed to retrieve all items, got %d out of %d requested keys after %d tries."
                    "Please check if one of the tables for the missing keys need more capacity."
                    % (len(retrieved), len(entity_keys), tries)
                )
        else:
            break

    return retrieved


def _boto3_config(online_config: DynamoDBOnlineStoreConfig):
    return botocore.client.Config(
        max_pool_connections=online_config.boto3_max_pool_connections
        if online_config.boto3_read_timeout
        else 10,
        connect_timeout=1,
        read_timeout=online_config.boto3_read_timeout / 1000.0
        if online_config.boto3_read_timeout
        else 1,
        retries={"mode": "standard", "total_max_attempts": 3},
    )


def _initialize_dynamodb_client(online_config: DynamoDBOnlineStoreConfig):
    if online_config.iam_role is not None:
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=online_config.iam_role, RoleSessionName="AssumeRoleFeastSession"
        )
        credentials = assumed_role_object["Credentials"]
        if online_config.dax_cluster_endpoint is not None:
            return amazondax.AmazonDaxClient(
                endpoint_url=online_config.dax_cluster_endpoint,
                region_name=online_config.region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                config=_boto3_config(online_config),
            )
        else:
            return boto3.client("dynamodb", config=_boto3_config(online_config),)

    else:
        if online_config.dax_cluster_endpoint is not None:
            return amazondax.AmazonDaxClient(
                endpoint_url=online_config.dax_cluster_endpoint,
                region_name=online_config.region,
                config=_boto3_config(online_config),
            )
        else:
            return boto3.client(
                "dynamodb",
                region_name=online_config.region,
                config=_boto3_config(online_config),
            )


def _initialize_dynamodb_resource(online_config: DynamoDBOnlineStoreConfig):
    if online_config.iam_role is not None:
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=online_config.iam_role, RoleSessionName="AssumeRoleFeastSession"
        )
        credentials = assumed_role_object["Credentials"]
        return boto3.resource(
            "dynamodb",
            region_name=online_config.region,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            config=_boto3_config(online_config),
        )
    else:
        return boto3.resource(
            "dynamodb",
            region_name=online_config.region,
            config=_boto3_config(online_config),
        )


def _initialize_dynamodb_resource_for_writes(online_config: DynamoDBOnlineStoreConfig):
    if online_config.iam_role is not None:
        sts_client = boto3.client("sts")
        assumed_role_object = sts_client.assume_role(
            RoleArn=online_config.iam_role, RoleSessionName="AssumeRoleFeastSession"
        )
        credentials = assumed_role_object["Credentials"]
        if online_config.dax_cluster_endpoint is not None:
            return amazondax.AmazonDaxClient.resource(
                endpoint_url=online_config.dax_cluster_endpoint,
                region_name=online_config.region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                config=_boto3_config(online_config),
            )
        else:
            return boto3.resource(
                "dynamodb",
                region_name=online_config.region,
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                config=_boto3_config(online_config),
            )
    else:
        if online_config.dax_cluster_endpoint is not None:
            return amazondax.AmazonDaxClient.resource(
                endpoint_url=online_config.dax_cluster_endpoint,
                region_name=online_config.region,
                config=_boto3_config(online_config),
            )
        else:
            return boto3.resource(
                "dynamodb",
                region_name=online_config.region,
                config=_boto3_config(online_config),
            )


def _get_table_name(config: RepoConfig, table: FeatureView) -> str:
    return f"{config.project}.{table.name}"


def _delete_table_idempotent(
    dynamodb_resource, table_name: str,
):
    try:
        table = dynamodb_resource.Table(table_name)
        table.delete()
        logger.info(f"Dynamo table {table_name} was deleted")
    except ClientError as ce:
        # If the table deletion fails with ResourceNotFoundException,
        # it means the table has already been deleted.
        # Otherwise, re-raise the exception
        if ce.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        else:
            logger.warning(f"Trying to delete table that doesn't exist: {table_name}")


class DynamoDBTable(InfraObject):
    """
    A DynamoDB table managed by Feast.

    Attributes:
        name: The name of the table.
        region: The region of the table.
    """

    region: str

    def __init__(self, name: str, region: str):
        super().__init__(name)
        self.region = region

    def to_infra_object_proto(self) -> InfraObjectProto:
        dynamodb_table_proto = self.to_proto()
        return InfraObjectProto(
            infra_object_class_type=DYNAMODB_INFRA_OBJECT_CLASS_TYPE,
            dynamodb_table=dynamodb_table_proto,
        )

    def to_proto(self) -> Any:
        dynamodb_table_proto = DynamoDBTableProto()
        dynamodb_table_proto.name = self.name
        dynamodb_table_proto.region = self.region
        return dynamodb_table_proto

    @staticmethod
    def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
        return DynamoDBTable(
            name=infra_object_proto.dynamodb_table.name,
            region=infra_object_proto.dynamodb_table.region,
        )

    @staticmethod
    def from_proto(dynamodb_table_proto: DynamoDBTableProto) -> Any:
        return DynamoDBTable(
            name=dynamodb_table_proto.name, region=dynamodb_table_proto.region,
        )

    def update(self):
        dynamodb_client = _initialize_dynamodb_client(region=self.region)
        dynamodb_resource = _initialize_dynamodb_resource(region=self.region)

        try:
            dynamodb_resource.create_table(
                TableName=f"{self.name}",
                KeySchema=[{"AttributeName": "entity_id", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "entity_id", "AttributeType": "S"}
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError as ce:
            # If the table creation fails with ResourceInUseException,
            # it means the table already exists or is being created.
            # Otherwise, re-raise the exception
            if ce.response["Error"]["Code"] != "ResourceInUseException":
                raise

        dynamodb_client.get_waiter("table_exists").wait(TableName=f"{self.name}")

    def teardown(self):
        dynamodb_resource = _initialize_dynamodb_resource(region=self.region)
        _delete_table_idempotent(dynamodb_resource, self.name)
