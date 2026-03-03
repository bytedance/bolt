#!/usr/bin/env python3
from pypaimon import CatalogFactory
from pypaimon.catalog.catalog_exception import (
    TableNotExistException,
)
from typing import Any
from pypaimon.table.row.generic_row import GenericRowSerializer
import pyarrow as pa
from pypaimon import Schema
from pathlib import Path
import pandas as pd
from argparse import ArgumentParser

def write_to_table(table: Any, data: pd.DataFrame):
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    table_write.write_pandas(data)
    commit_messages = table_write.prepare_commit()
    table_commit.commit(commit_messages)
    table_write.close()
    table_commit.close()

def create_table(catalog, database: str, table_name: str, schema: pa.Schema) -> tuple[bool, Any]:
    '''
    Create a table in the catalog if it does not exist

    Returns:
        (bool, table): True if the table was created. False if it was not
        created because it already existed, plus the created table object.
    '''
    try:
        table = catalog.get_table(f'{database}.{table_name}')
        return (False, table)
    except TableNotExistException:
        catalog.create_table(
            identifier=f'{database}.{table_name}',
            schema=schema,
            ignore_if_exists=False,
        )
        table = catalog.get_table(f'{database}.{table_name}')
        return (True, table)

def basic_table(catalog):
    pa_schema = pa.schema([
        ('id', pa.int64()),
    ])
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=[],
        options={'bucket': '2'},
        comment='my test table')
    data = {
    'id': [1, 2, 3],
    }
    dataframe = pd.DataFrame(data)
    (table_created, table) = create_table(
        catalog=catalog,
        database='test_db',
        table_name='basic',
        schema=schema
    )
    if not table_created:
        return
    write_to_table(table, dataframe)

def append_only_multiple_append(catalog):
    pa_schema = pa.schema([
        ('id', pa.int64()),
    ])
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=[],
        options={'bucket': '2'},
        comment='my test table')
    data_1 = {
    'id': [4, 5, 6],
    }
    data_2 = {
    'id': [7, 8, 9],
    }
    dataframe_1 = pd.DataFrame(data_1)
    dataframe_2 = pd.DataFrame(data_2)
    (table_created, table) = create_table(
        catalog=catalog,
        database='test_db',
        table_name='append_only_multiple_append',
        schema=schema
    )
    if not table_created:
        return

    # write dataset 1
    write_to_table(table, dataframe_1)

    # write dataset 2
    write_to_table(table, dataframe_2)

def pk_no_overwrite(catalog):
    pa_schema = pa.schema([
        ('id', pa.int64()),
    ])
    schema = Schema.from_pyarrow_schema(
        pa_schema=pa_schema,
        partition_keys=[],
        primary_keys=['id'],
        options={'bucket': '2'},
        comment='my test table')
    data_1 = {
    'id': [10, 11, 12],
    }
    data_2 = {
    'id': [13, 14, 15],
    }
    dataframe_1 = pd.DataFrame(data_1)
    dataframe_2 = pd.DataFrame(data_2)
    (table_created, table) = create_table(
        catalog=catalog,
        database='test_db',
        table_name='pk_no_overwrite',
        schema=schema
    )
    if not table_created:
        return

    # write dataset 1
    write_to_table(table, dataframe_1)

    # write dataset 2
    write_to_table(table, dataframe_2)


def main():
    parser = ArgumentParser()
    parser.add_argument('-b', '--base-path', default=str(Path(__file__).parent / 'test_warehouse'))
    args = parser.parse_args()
    base_path = args.base_path
    print(f'warehouse base path: {base_path}')
    catalog_options = {
        'warehouse': f'file://{base_path}'
    }
    catalog = CatalogFactory.create(catalog_options)

    catalog.create_database(
    name='test_db',
    ignore_if_exists=True,
    )

    tables = [
        basic_table,
        append_only_multiple_append,
        pk_no_overwrite,
    ]
    for create_table in tables:
        create_table(catalog)

if __name__ == '__main__':
    main()
