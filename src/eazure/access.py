import os
from dotenv import load_dotenv
from typing import Literal
from azure.cosmosdb.table.tableservice import TableService


def get_access(
    var_name: str, access_type: Literal["blob", "table"] = "blob"
) -> str | TableService:
    """
    Imports an Azure data lake storage connection string including
    the associated access key from a local .ENV file.

    Args:
        filepath (str): path to a TXT file containing
        the connection string

    Returns:
        str: the connection string to pass on to other functions
        such as read_blob() and write_blob()

    Args:
        var_name (str): name of the variable in the .ENV file that
        contains the connection string for Azure
        access_type (Literal['blob', 'table'], optional):
        whether to return the access string for accessing blobs or
        a pair of account name and access key for accessing tables.
        Defaults to "blob".

    Returns:
        str|TableService[str, str]: connection string if
        "access_type" is "blob", TableService if "access_type"
        is "table".
    """
    # Importing connection strings from the .ENV file
    load_dotenv()
    conn_string = os.getenv(var_name)
    if not conn_string:
        raise ValueError("Connectiong string not found in .ENV file.")

    # Return connection string if connecting to blob storage
    if access_type == "blob":
        return conn_string

    # Return table service string if connecting to Azure tables
    if access_type == "table":
        parts = dict(
            item.split("=", 1) for item in conn_string.split(";") if "=" in item
        )
        account_name = parts.get("AccountName")
        account_key = parts.get("AccountKey")
        table_service = TableService(account_name=account_name, account_key=account_key)
        return table_service
