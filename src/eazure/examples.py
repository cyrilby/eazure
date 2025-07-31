from eazure.access import get_access
from eazure.files import read_blob, write_blob, filter_blob
from eazure.tables import (
    table_exists,
    create_table,
    write_df_to_azure_table_batch,
    add_keys_to_df,
    delete_all_rows_batch,
    delete_table_if_exists,
    query_entity,
    query_entities,
    rename_table,
    copy_column,
    delete_column,
    rename_column,
)
import pandas as pd
import datetime as dt


# %% Defining connection string and container name

# Importing connection string including access key
conn_string = get_access("AZURE_ACCESS_KEY")

# Specifying which container to use
container_name = "eazure"


# %% Reading & writing pandas data frames

# Reading a df stored in an Excel file
data = read_blob(conn_string, container_name, "test.xlsx")
print(data.head(5))

# Reading a df stored in a CSV file
data = read_blob(conn_string, container_name, "test.csv")
print(data.head(5))

# Reading a df stored in a pickle file
data = read_blob(conn_string, container_name, "test.pkl")
print(data.head(5))

# Reading a df stored in a parquet file
data = read_blob(conn_string, container_name, "test.parquet")
print(data.head(5))

# Writing a df to an Excel file
write_blob(
    data,
    conn_string,
    container_name,
    "Outputs/test.xlsx",
    sheet_name="Test",
    index=False,
)

# Writing a df to a CSV file
write_blob(data, conn_string, container_name, "Outputs/test.csv", sep=",", index=False)

# Writing a df to a pickle file
write_blob(data, conn_string, container_name, "Outputs/test.pkl")

# Writing a df to a parquet file
write_blob(data, conn_string, container_name, "Outputs/test.parquet")


# %% Reading and writing other types of objects

# Reading a Python list stored in a pickle file
test_list = read_blob(conn_string, container_name, "test_list.pkl")
print(test_list)

# Writing a Python list to a pickle file
write_blob(test_list, conn_string, container_name, "Outputs/test_list.pkl")

# Reading a Python dict stored in a JSON file
test_dict = read_blob(conn_string, container_name, "test_dict.json")
print(test_dict)

# Writing a Python dict to a JSON file
write_blob(test_dict, conn_string, container_name, "Outputs/test_dict.json")

# Reading a string stored in a text file
test_string = read_blob(conn_string, container_name, "test_string.txt")
print(test_string)

# Writing a string to a text file
write_blob(test_string, conn_string, container_name, "Outputs/test_string.txt")


# %% Cleaning up in Azure file storage

# Should this be enabled?
enable_cleanup = False

# Which files should be filtered and cleaned?
files_to_cleanup = [
    "container_demand.parquet",
    "vessel_demand.parquet",
    "vessel_demand_agg_quarter.parquet",
    "vessel_demand_agg_year.parquet",
    "macro_scenarios.parquet",
    "conversion_factors.parquet",
    "country_macro_data.parquet",
    "country_macro_model_stats.parquet",
]

# Which timestamps should be kept?
timestamps_to_keep = ["2024-01-09 11:41:15"]

# Applying the cleanup if so specified by the user
if enable_cleanup:
    conn_string = get_access("azure_conn.txt")
    for file in files_to_cleanup:
        print(f"Cleaning up in file: '{file}'...")
        filter_blob(
            conn_string, "containerdemand", file, {"DataUpdated": timestamps_to_keep}
        )


# %% Setting up connection to Azure tables and checking if a table exists

# Setting up the connection via a table service object
table_service = get_access("AZURE_ACCESS_KEY", "table")

# Checking whether a table exists
print("Does the table 'TestTable' exist?")
print(table_exists(table_service, "TestTable"))


# %% Creating a new Azure table, pushing data to it and deleting it

# Creating a new empty Azure table
create_table(table_service, "TestTable")

# Creating a timestamp to use as partition key
timestamp = dt.datetime.now()
timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

# Creating a temporary pandas df
list_names = ["Bianca", "Jeff", "Sarah"]
list_ages = [24, 29, 26]
temp_df = pd.DataFrame({"Name": list_names, "Age": list_ages})

# Adding partition key (based on timestamp) and row key to the table
# Note: these are required by Azure Tables
temp_df = add_keys_to_df(temp_df, timestamp)

# Appending the rows to the newly created Azure table
write_df_to_azure_table_batch(table_service, "TestTable", temp_df)

# Creating a new timestamp to use as partition key
timestamp = dt.datetime.now()
timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

# Creating a new temporary pandas df and adding keys
list_names = ["Jesus", "Wang", "Estrella"]
list_ages = [17, 31, 42]
temp_df2 = pd.DataFrame({"Name": list_names, "Age": list_ages})
temp_df2 = add_keys_to_df(temp_df2, timestamp)

# Appending the rows to the already existing Azure table
# Note: the default behavior with truncate=False
# will keep the already existing rows in the table
write_df_to_azure_table_batch(table_service, "TestTable", temp_df2)

# Overwriting the entire table with the new rows
write_df_to_azure_table_batch(table_service, "TestTable", temp_df2, True)

# Deleting all rows in a table
delete_all_rows_batch(table_service, "TestTable")

# Deleting the newly created table
# Note: it's not necessary to delete all rows before deleting a table
delete_table_if_exists(table_service, "TestTable")

# Re-creating the table and pushing all data to it with the newest timestamp
temp_df3 = add_keys_to_df(pd.concat([temp_df, temp_df2]), timestamp)
write_df_to_azure_table_batch(table_service, "TestTable", temp_df3)

# %% Renaming an Azure table

# Note: if a table with the same new name exists, it will be overwritten
# Furthermore, this operation cannot be performed using batches as the original
# partition key and row key will be lost, so it can be a bit slow with larger tables
rename_table(table_service, "TestTable", "EazureTest")


# %% Quering rows from an existing Azure Table

# Querying one specific row with known "PartitionKey" and "RowKey"
# Note: this returns a dictionary
part_key = timestamp
row_key = timestamp + "-0"
retrieved_data = query_entity(table_service, "EazureTest", part_key, row_key)
print(retrieved_data)

# Querying all rows
# Note: this returns a pandas dataframe by default
retrieved_data = query_entities(table_service, "EazureTest")
print(retrieved_data)


# %% Copying, deleting and renaming columns in an existing Azure table

# Copying a column
copy_column(table_service, "EazureTest", "Name", "PersonalName")

# Deleting a column
delete_column(table_service, "EazureTest", "PersonalName")

# Renaming a column
rename_column(table_service, "EazureTest", "Name", "FirstName")

# %%
