import pandas as pd
import numpy as np
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
from azure.cosmosdb.table.tablebatch import TableBatch


def table_exists(table_service: TableService, table_name: str) -> bool:
    """
    Checks whether a specified table already exists in Azure.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table we want to check

    Returns:
        bool: exists or not
    """
    return table_service.exists(table_name)


def create_table(table_service: TableService, table_name: str) -> None:
    """
    Creates a new table in Azure DLS tables if it doesn't already exist.
    Else, does nothing.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table we want to create
    """
    if not table_exists(table_service, table_name):
        table_service.create_table(table_name)
    else:
        print(f"Table '{table_name}' already exists. Please choose a different name.")


def delete_table_if_exists(table_service: TableService, table_name: str) -> None:
    """
    Deletes an Azure DLS table if it exists. Else, does nothing.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table we want to delete
    """
    if table_service.exists(table_name):
        table_service.delete_table(table_name)


def query_entity(
    table_service: TableService, table_name: str, partition_key: str, row_key: str
) -> Entity:
    """
    Queries a specific entity from an Azure DSL table by using a
    paritition key and a row key to identify the entry in question.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the Azure table to query
        partition_key (str): partition key to use as ID
        row_key (str): row key to use as ID

    Returns:
        Entity: Azure table entity
    """
    entity = table_service.get_entity(table_name, partition_key, row_key)
    return entity


def query_entities(
    table_service: TableService,
    table_name: str,
    return_df: bool = True,
    filter_expression: str = "",
) -> pd.DataFrame:
    """
    Queries all or a filtered subset of entities present in an
    Azure DLS table.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the Azure table to query
        return_df (bool): whether to return a pandas dataframe
        instead of a list of dictionaries (defaults to pandas df)
        filter_expression (str): filter expression (optional)

    Returns:
        list: list of all entities in the Azure table
    """
    entities = table_service.query_entities(table_name, filter=filter_expression)
    if return_df:
        entities = pd.DataFrame(entities)
    return entities


def delete_all_rows(table_service: TableService, table_name: str) -> None:
    """
    Deletes all rows in an Azure DLS table one by one.
    Only suitable for use with smaller table sizes, else too slow.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table where we want to delete all rows
    """
    # Query all entities in the table
    entities = table_service.query_entities(table_name)

    # Delete each entity
    for entity in entities:
        table_service.delete_entity(
            table_name, entity["PartitionKey"], entity["RowKey"]
        )


def delete_all_rows_batch(table_service: TableService, table_name: str) -> None:
    """
    Deletes all rows in an Azure DLS table by using batches
    to speed up the process.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table where we want to delete all rows
    """
    # Query all entities in the table
    entities = table_service.query_entities(table_name)

    # Initialize a new batch
    batch = TableBatch()

    # Track the current PartitionKey
    current_partition_key = None

    # Track the number of entities in the current batch
    batch_count = 0

    for entity in entities:
        # If the PartitionKey changes or the batch size reaches 100, commit the current batch and start a new one
        if current_partition_key != entity["PartitionKey"] or batch_count == 100:
            if current_partition_key is not None:
                table_service.commit_batch(table_name, batch)
            batch = TableBatch()
            current_partition_key = entity["PartitionKey"]
            batch_count = 0

        # Add the delete operation to the batch
        batch.delete_entity(entity["PartitionKey"], entity["RowKey"])
        batch_count += 1

    # Commit the last batch if it has any entities
    if batch_count > 0:
        table_service.commit_batch(table_name, batch)


def write_df_to_azure_table(
    table_service: TableService,
    table_name: str,
    df: pd.DataFrame,
    truncate: bool = False,
) -> None:
    """
    Appends all rows from a pandas dataframe to an Azure DLS table.
    Does this on a one-by-one basis, so only suitable for small tables.
    Otherwise, the performance will too slow.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table to append rows to
        df (pd.DataFrame): pandas df to get the rows from
        truncate (bool): whether to delete all rows from the existing
        table (if it exists) before appending new data.
    """
    # If table does not exist, we need to create it first
    if not table_exists(table_service, table_name):
        create_table(table_service, table_name)
    # Delete all rows if so specified
    if truncate:
        delete_all_rows_batch(table_service, table_name)
    # For existing tables, we simply append the rows
    data = df.to_dict("records")
    for row in data:
        entity = Entity()
        entity.PartitionKey = row["PartitionKey"]
        entity.RowKey = row["RowKey"]
        for key, value in row.items():
            setattr(entity, key, value)
        table_service.insert_entity(table_name, entity)


def write_df_to_azure_table_batch(
    table_service: TableService,
    table_name: str,
    df: pd.DataFrame,
    truncate: bool = False,
) -> None:
    """
    Appends all rows from a pandas dataframe to an Azure DLS table.
    Does this using batches, which speeds up the process when working
    with larger data tables.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the table to append rows to
        df (pd.DataFrame): pandas df to get the rows from
        truncate (bool): whether to delete all rows from the existing
        table (if it exists) before appending new data.
    """
    # If table does not exist, we need to create it first
    if not table_exists(table_service, table_name):
        create_table(table_service, table_name)
    # Delete all rows if so specified
    if truncate:
        delete_all_rows_batch(table_service, table_name)
    # For existing tables, we simply append the rows
    data = df.to_dict("records")
    batch = TableBatch()
    batch_count = 0  # Keep track of the number of entities in the batch
    for i, row in enumerate(data):
        entity = Entity()
        entity.PartitionKey = row["PartitionKey"]
        entity.RowKey = row["RowKey"]
        for key, value in row.items():
            setattr(entity, key, value)
        batch.insert_entity(entity)
        batch_count += 1  # Increment the count
        if (
            batch_count == 100
        ):  # If the batch size reaches 100, commit the batch and start a new one
            table_service.commit_batch(table_name, batch)
            batch = TableBatch()
            batch_count = 0  # Reset the count
    if batch_count > 0:  # Commit the last batch if it has any entities
        table_service.commit_batch(table_name, batch)


def add_keys_to_df(df: pd.DataFrame, partition_key: str) -> pd.DataFrame:
    """
    Adds a string-based paritition key to a pandas dataframe
    as well as a row key based on the partition key and the
    row number. This is one possible implementation of what
    needs to be done before a dataframe can be uploaded to
    Azure tables.

    Args:
        df (pd.DataFrame): df that needs keys to be added
        partition_key (str): value to be used as the
        partition key for Azure

    Returns:
        pd.DataFrame: _description_
    """

    max_digits = len(str(len(df)))
    df["PartitionKey"] = partition_key
    df["RowKey"] = np.arange(len(df))
    df["RowKey"] = df["RowKey"].astype(str).str.zfill(max_digits)
    df["RowKey"] = df["PartitionKey"] + "-" + df["RowKey"]
    return df


def rename_table(table_service: TableService, old_name: str, new_name: str) -> None:
    """
    Renames an Azure table. In reality, the process is more complicated
    than that: first, we need to create a new table; then, we need to copy
    all entities from the existing table to the new one and finally, we
    need to delete the old Azure table. This process cannot be done using
    batches, which is why the process may take a long time for large tables.

    Args:
        table_service (TableService): Azure table service object
        old_name (str): name of the existing Azure table
        new_name (str): new name for the Azure table
    """
    # Create a new table with the new name
    # Note: if table already exists, it will be deleted!
    delete_table_if_exists(table_service, new_name)
    create_table(table_service, new_name)

    # Query all entities from the old table
    entities = query_entities(table_service, old_name)
    entities.drop(columns=["Timestamp"], inplace=True)
    # print(entities.head(4))

    # Insert all entities into the new table
    write_df_to_azure_table(table_service, new_name, entities, True)

    # Delete the old table
    delete_table_if_exists(table_service, old_name)


def copy_column(
    table_service: TableService, table_name: str, old_column: str, new_column: str
) -> None:
    """
    Copies a column in all entities in an Azure table.
    This process cannot be done using batches, which is why the
    process may take a long time for large tables.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the Azure table
        old_column (str): name of the column to copy
        new_column (str): name of the new column
    """
    # Query all entities from the table
    entities = table_service.query_entities(table_name)

    for entity in entities:
        # Check if the entity has the column
        if old_column in entity:
            # Create a new entity with the new column
            new_entity = entity.copy()
            new_entity[new_column] = new_entity[old_column]

            # Replace the old entity with the new one
            table_service.update_entity(table_name, new_entity)


def delete_column(
    table_service: TableService, table_name: str, column_name: str
) -> None:
    """
    Deletes a column from all entities in an Azure table.
    This process cannot be done using batches, which is why the
    process may take a long time for large tables.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the Azure table
        column_name (str): name of the column to delete
    """
    # Query all entities from the table
    entities = table_service.query_entities(table_name)

    for entity in entities:
        # Check if the entity has the column
        if column_name in entity:
            # Create a new entity without the column
            new_entity = {k: v for k, v in entity.items() if k != column_name}

            # Replace the old entity with the new one
            table_service.update_entity(table_name, new_entity)


def rename_column(
    table_service: TableService, table_name: str, old_column: str, new_column: str
) -> None:
    """
    Renames a column in all entities in an Azure table.
    This process cannot be done using batches, which is why the
    process may take a long time for large tables.

    Args:
        table_service (TableService): Azure table service object
        table_name (str): name of the Azure table
        old_column (str): name of the column to rename
        new_column (str): new name for the column
    """
    # Copy the old column to the new column
    copy_column(table_service, table_name, old_column, new_column)

    # Delete the old column
    delete_column(table_service, table_name, old_column)
