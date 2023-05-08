import re
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union, Type, Tuple, Callable
from typing_extensions import TypeAlias

import agate
from dbt.contracts.relation import RelationType

import dbt
import dbt.exceptions

from dbt.adapters.base import AdapterConfig, PythonJobHelper
from dbt.adapters.base.impl import catch_as_completed
from dbt.contracts.connection import AdapterResponse
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.spark import SparkConnectionManager
from dbt.adapters.spark import SparkRelation
from dbt.adapters.spark import SparkColumn
from dbt.adapters.spark.python_submissions import (
    JobClusterPythonJobHelper,
    AllPurposeClusterPythonJobHelper,
)
from dbt.adapters.base import BaseRelation
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.events import AdapterLogger
from dbt.flags import get_flags
from dbt.utils import executor, AttrDict

logger = AdapterLogger("Spark")

GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME = "get_columns_in_relation_raw"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
LIST_RELATIONS_SHOW_TABLES_MACRO_NAME = "list_relations_show_tables_without_caching"
DESCRIBE_TABLE_EXTENDED_MACRO_NAME = "describe_table_extended_without_caching"
DROP_RELATION_MACRO_NAME = "drop_relation"
FETCH_TBL_PROPERTIES_MACRO_NAME = "fetch_tbl_properties"

KEY_TABLE_OWNER = "Owner"
KEY_TABLE_STATISTICS = "Statistics"

TABLE_OR_VIEW_NOT_FOUND_MESSAGES = (
    "[TABLE_OR_VIEW_NOT_FOUND]",
    "Table or view not found",
    "NoSuchTableException",
)


@dataclass
class SparkConfig(AdapterConfig):
    file_format: str = "parquet"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


@dataclass(frozen=True)
class RelationInfo:
    table_schema: str
    table_name: str
    columns: List[Tuple[str, str]]
    properties: Dict[str, str]


class SparkAdapter(SQLAdapter):
    COLUMN_NAMES = (
        "table_database",
        "table_schema",
        "table_name",
        "table_type",
        "table_comment",
        "table_owner",
        "column_name",
        "column_index",
        "column_type",
        "column_comment",
        "stats:bytes:label",
        "stats:bytes:value",
        "stats:bytes:description",
        "stats:bytes:include",
        "stats:rows:label",
        "stats:rows:value",
        "stats:rows:description",
        "stats:rows:include",
    )
    INFORMATION_COLUMN_REGEX = re.compile(r" \|-- (.*): (.*) \(nullable = (.*)\)")
    HUDI_METADATA_COLUMNS = [
        "_hoodie_commit_time",
        "_hoodie_commit_seqno",
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_name",
    ]

    Relation: TypeAlias = SparkRelation
    Column: TypeAlias = SparkColumn
    ConnectionManager: TypeAlias = SparkConnectionManager
    AdapterSpecificConfigs: TypeAlias = SparkConfig

    @classmethod
    def date_function(cls) -> str:
        return "current_timestamp()"

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "bigint"

    @classmethod
    def convert_date_type(cls, agate_table, col_idx):
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "time"

    @classmethod
    def convert_datetime_type(cls, agate_table, col_idx):
        return "timestamp"

    def quote(self, identifier):
        return "`{}`".format(identifier)

    def add_schema_to_cache(self, schema) -> str:
        """Cache a new schema in dbt. It will show up in `list relations`."""
        if schema is None:
            name = self.nice_connection_name()
            raise dbt.exceptions.CompilationError(
                "Attempted to cache a null schema for {}".format(name)
            )
        if get_flags().USE_CACHE:  # type: ignore
            self.cache.add_schema(None, schema)
        # so jinja doesn't render things
        return ""

    def _get_relation_information(self, row: agate.Row) -> RelationInfo:
        """relation info was fetched with SHOW TABLES EXTENDED"""
        try:
            # Example lines:
            # Database: dbt_schema
            # Table: names
            # Owner: fokkodriesprong
            # Created Time: Mon May 08 18:06:47 CEST 2023
            # Last Access: UNKNOWN
            # Created By: Spark 3.3.2
            # Type: MANAGED
            # Provider: hive
            # Table Properties: [transient_lastDdlTime=1683562007]
            # Statistics: 16 bytes
            # Schema: root
            # |-- idx: integer (nullable = false)
            # |-- name: string (nullable = false)
            table_properties = {}
            columns = []
            _schema, name, _, information_blob = row
            for line in information_blob.split("\n"):
                if line:
                    if line.startswith(" |--"):
                        # A column
                        match = self.INFORMATION_COLUMN_REGEX.match(line)
                        if match:
                            columns.append((match[1], match[2]))
                        else:
                            logger.warning(f"Could not parse: {line}")
                    else:
                        # A property
                        parts = line.split(": ", maxsplit=2)
                        table_properties[parts[0]] = parts[1]

        except ValueError:
            raise dbt.exceptions.DbtRuntimeError(
                f'Invalid value from "show tables extended ...", got {len(row)} values, expected 4'
            )

        return RelationInfo(_schema, name, columns, table_properties)

    def _get_relation_information_using_describe(self, row: agate.Row) -> RelationInfo:
        """Relation info fetched using SHOW TABLES and an auxiliary DESCRIBE statement"""
        try:
            _schema, name, _ = row
        except ValueError:
            raise dbt.exceptions.DbtRuntimeError(
                f'Invalid value from "show tables ...", got {len(row)} values, expected 3'
            )

        table_name = f"{_schema}.{name}"
        try:
            table_results = self.execute_macro(
                DESCRIBE_TABLE_EXTENDED_MACRO_NAME, kwargs={"table_name": table_name}
            )
        except dbt.exceptions.DbtRuntimeError as e:
            logger.debug(f"Error while retrieving information about {table_name}: {e.msg}")
            table_results = AttrDict()

        # idx	int
        # name	string
        #
        # # Partitioning
        # Not partitioned
        #
        # # Metadata Columns
        # _spec_id	int
        # _partition	struct<>
        # _file	string
        # _pos	bigint
        # _deleted	boolean
        #
        # # Detailed Table Information
        # Name	sandbox.dbt_tabular3.names
        # Location	s3://tabular-wh-us-east-1/6efbcaf4-21ae-499d-b340-3bc1a7003f52/d2082e32-d2bd-4484-bb93-7bc445c1c6bb
        # Provider	iceberg

        # Wrap it in an iter, so we continue reading the properties from where we stopped reading columns
        table_results_itr = iter(table_results)

        # First the columns
        columns = []
        for info_row in table_results_itr:
            if info_row[0] == "":
                break
            columns.append((info_row[0], info_row[1]))

        # Next all the properties
        table_properties = {}
        for info_row in table_results_itr:
            info_type, info_value, _ = info_row
            if not info_type.startswith("#") and info_type != "":
                table_properties[info_type] = info_value

        return RelationInfo(_schema, name, columns, table_properties)

    def _build_spark_relation_list(
        self,
        row_list: agate.Table,
        relation_info_func: Callable[[agate.Row], RelationInfo],
    ) -> List[BaseRelation]:
        """Aggregate relations with format metadata included."""
        relations: List[BaseRelation] = []
        for row in row_list:
            relation = relation_info_func(row)

            rel_type: RelationType = (
                RelationType.View
                if relation.properties.get("type") == "VIEW"
                else RelationType.Table
            )
            is_delta: bool = relation.properties.get("provider") == "delta"
            is_hudi: bool = relation.properties.get("provider") == "hudi"
            is_iceberg: bool = relation.properties.get("provider") == "iceberg"

            relations.append(
                self.Relation.create(
                    schema=relation.table_schema,
                    identifier=relation.table_name,
                    type=rel_type,
                    is_delta=is_delta,
                    is_iceberg=is_iceberg,
                    is_hudi=is_hudi,
                    columns=relation.columns,
                    properties=relation.properties,
                )
            )

        return relations

    def list_relations_without_caching(self, schema_relation: BaseRelation) -> List[BaseRelation]:
        """Distinct Spark compute engines may not support the same SQL featureset. Thus, we must
        try different methods to fetch relation information."""

        kwargs = {"schema_relation": schema_relation}

        try:
            # Default compute engine behavior: show tables extended
            show_table_extended_rows = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
            return self._build_spark_relation_list(
                row_list=show_table_extended_rows,
                relation_info_func=self._get_relation_information,
            )
        except dbt.exceptions.DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            # Iceberg compute engine behavior: show table
            elif "SHOW TABLE EXTENDED is not supported for v2 tables" in errmsg:
                # this happens with spark-iceberg with v2 iceberg tables
                # https://issues.apache.org/jira/browse/SPARK-33393
                try:
                    # Iceberg behavior: 3-row result of relations obtained
                    show_table_rows = self.execute_macro(
                        LIST_RELATIONS_SHOW_TABLES_MACRO_NAME, kwargs=kwargs
                    )
                    return self._build_spark_relation_list(
                        row_list=show_table_rows,
                        relation_info_func=self._get_relation_information_using_describe,
                    )
                except dbt.exceptions.DbtRuntimeError as e:
                    description = "Error while retrieving information about"
                    logger.debug(f"{description} {schema_relation}: {e.msg}")
                    return []
            else:
                logger.debug(
                    f"Error while retrieving information about {schema_relation}: {errmsg}"
                )
                return []

    def get_relation(self, database: str, schema: str, identifier: str) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None  # type: ignore

        return super().get_relation(database, schema, identifier)

    def parse_describe_extended(
        self, relation: SparkRelation, raw_rows: AttrDict
    ) -> List[SparkColumn]:
        # Convert the Row to a dict
        raw_table_stats = relation.properties.get(KEY_TABLE_STATISTICS)
        table_stats = SparkColumn.convert_table_stats(raw_table_stats)
        return [
            SparkColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=relation.properties.get(KEY_TABLE_OWNER, ""),
                table_stats=table_stats,
                column=column_name,
                column_index=idx,
                dtype=column_type,
            )
            for idx, (column_name, column_type) in enumerate(relation.columns)
        ]

    def get_columns_in_relation(self, relation: BaseRelation) -> List[SparkColumn]:
        columns = []
        try:
            rows: AttrDict = self.execute_macro(
                GET_COLUMNS_IN_RELATION_RAW_MACRO_NAME, kwargs={"relation": relation}
            )
            columns = self.parse_describe_extended(relation, rows)  # type: ignore
        except dbt.exceptions.DbtRuntimeError as e:
            # spark would throw error when table doesn't exist, where other
            # CDW would just return and empty list, normalizing the behavior here
            errmsg = getattr(e, "msg", "")
            found_msgs = (msg in errmsg for msg in TABLE_OR_VIEW_NOT_FOUND_MESSAGES)
            if any(found_msgs):
                pass
            else:
                raise e

        # strip hudi metadata columns.
        columns = [x for x in columns if x.name not in self.HUDI_METADATA_COLUMNS]
        return columns

    def parse_columns_from_information(self, relation: SparkRelation) -> List[SparkColumn]:
        owner = relation.properties.get(KEY_TABLE_OWNER, "")
        columns = []
        table_stats = SparkColumn.convert_table_stats(
            relation.properties.get(KEY_TABLE_STATISTICS)
        )
        for match_num, (column_name, column_type) in enumerate(relation.columns):
            column = SparkColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.table,
                table_type=relation.type,
                column_index=match_num,
                table_owner=owner,
                column=column_name,
                dtype=column_type,
                table_stats=table_stats,
            )
            columns.append(column)
        return columns

    def _get_columns_for_catalog(self, relation: BaseRelation) -> Iterable[Dict[str, Any]]:
        columns = self.parse_columns_from_information(relation)  # type: ignore

        for column in columns:
            # convert SparkColumns into catalog dicts
            as_dict = column.to_column_dict()
            as_dict["column_name"] = as_dict.pop("column", None)
            as_dict["column_type"] = as_dict.pop("dtype")
            as_dict["table_database"] = None
            yield as_dict

    def get_properties(self, relation: Relation) -> Dict[str, str]:
        properties = self.execute_macro(
            FETCH_TBL_PROPERTIES_MACRO_NAME, kwargs={"relation": relation}
        )
        return dict(properties)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            raise dbt.exceptions.CompilationError(
                f"Expected only one database in get_catalog, found " f"{list(schema_map)}"
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []  # type: ignore
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(
                        tpe.submit_connected(
                            self,
                            schema,
                            self._get_one_catalog,
                            info,
                            [schema],
                            manifest,
                        )
                    )
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self,
        information_schema,
        schemas,
        manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            raise dbt.exceptions.CompilationError(
                f"Expected only one schema in spark _get_one_catalog, found " f"{schemas}"
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", str(relation))
            columns.extend(self._get_columns_for_catalog(relation))
        return agate.Table.from_object(columns, column_types=DEFAULT_TYPE_TESTER)

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql

    # This is for use in the test suite
    # Spark doesn't have 'commit' and 'rollback', so this override
    # doesn't include those commands.
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if fetch == "one":
                if hasattr(cursor, "fetchone"):
                    return cursor.fetchone()
                else:
                    # AttributeError: 'PyhiveConnectionWrapper' object has no attribute 'fetchone'
                    return cursor.fetchall()[0]
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False

    def generate_python_submission_response(self, submission_result: Any) -> AdapterResponse:
        return self.connections.get_response(None)

    @property
    def default_python_submission_method(self) -> str:
        return "all_purpose_cluster"

    @property
    def python_submission_helpers(self) -> Dict[str, Type[PythonJobHelper]]:
        return {
            "job_cluster": JobClusterPythonJobHelper,
            "all_purpose_cluster": AllPurposeClusterPythonJobHelper,
        }

    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["Principal"]
            privilege = row["ActionType"]
            object_type = row["ObjectType"]

            # we only want to consider grants on this object
            # (view or table both appear as 'TABLE')
            # and we don't want to consider the OWN privilege
            if object_type == "TABLE" and privilege != "OWN":
                if privilege in grants_dict.keys():
                    grants_dict[privilege].append(grantee)
                else:
                    grants_dict.update({privilege: [grantee]})
        return grants_dict


# spark does something interesting with joins when both tables have the same
# static values for the join condition and complains that the join condition is
# "trivial". Which is true, though it seems like an unreasonable cause for
# failure! It also doesn't like the `from foo, bar` syntax as opposed to
# `from foo cross join bar`.
COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} EXCEPT
             SELECT {columns} FROM {relation_b})
             UNION ALL
            (SELECT {columns} FROM {relation_b} EXCEPT
             SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a
    cross join table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
cross join diff_count
""".strip()
