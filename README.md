
# Delta Share Utils

Delta Sharing utils includes set of python files that makes it very easy to sync tables shared using Delta Sharing protocol.

## How it works

These utilities are organised in 2 main python classes, data_provider.py and data_recipient.py

#### Examples for DeltaShareProvider Class (by the Data Sharer)
```python
# create a DeltaShareProvider instance for a share 'my_share' and to be shared with a recipient 'my_recipient'
# after running this code, you will get an activation link to be shared with your recipient to download the share file
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient")

# share all the tables in a database with my recipients
dsp.share_database(database="my_database")

# share a table with Change Data Feed enabled so the data recipient can incrementally load the data
dsp.share_table(table="my_database.my_table", enable_cdf=True)
```

#### Examples for DeltaShareRecipient Class (by the Data Consumer)
```python
# create a DeltaShareRecipient instance and point it to the share file location that was downloaded form the activation link
dsr = DeltaShareRecipient('/dbfs/FileStore/tables/amr_azure_share.share')

# this will display a list of all shares, and what tables are shared
display(dsr.discover())

# this will start sync the data from the data sharer to the data recipients, diffrent options can be used:
# cache_locally (bool, optional): Whether to cache the table locally. Defaults to False.
# refresh_incrementally (bool, optional): Whether to refresh the cache incrementally, note CDF must be enabled on the source table. Defaults to False.
# clear_previous_cache (bool, optional): Whether to clear the previous cache (warning: this will drop the tables and clear all content). Defaults to False.
# clear_sync_history (bool, optional): Whether to clear the sync history. Defaults to False.
# primary_keys (dict, optional): The primary keys for the the tables inside the share, this is needed for incremental updates to work. Defaults is empty {}
dsr.share_sync(cache_locally=True, refresh_incrementally=True,\
               clear_previous_cache=True, clear_sync_history=True,\
               primary_keys = {'db1.table1':'id', 'db1.table2':'idx'})
```

### DeltaShareProvider Class (data_provider.py)
A python class that provides a simplified way to share Delta tables and databases between different Databricks workspaces. The class allows you to share catalogs, databases, and tables to a recipient by using a Databricks share.

### Prerequisites
This class is built to be used in a Databricks environment. Ensure that you have access to a Databricks workspace, and you are familiar with Python, SQL, and Delta.
The DeltaShareProvider requires a metastore admin previlige so you can create shares and add recipients.

### Installation
You can copy the entire code and use it in your Databricks notebook or import the class to your codebase.

### DeltaShareProvider API Reference
The class allows you to share catalogs, databases, and tables to a recipient using a Databricks share. You can use the following methods to perform the actions.

```python
DeltaShareProvider(share:str, recipient:str, recipient_databricks_id:str="", drop_if_exists:bool=False)
```
Create an instance of the class with the following parameters:

**share**: The name of the Databricks share.

**recipient**: The name of the recipient who will receive the shared data.

**recipient_databricks_id**: (optional) The identifier of the Databricks workspace where the recipient resides. Required when sharing with external Databricks workspaces.

**drop_if_exists**: (optional) Set to True to delete the recipient and the share if they already exist.

```python
share_catalog(catalog:str, enable_cdf:bool=False)
```
Share all the databases in a catalog to the recipient. You can enable Change Data Feed (CDF) by setting enable_cdf to True.

```python
unshare_catalog(catalog:str)
```
Remove all the databases in a catalog from the share.

```python
share_database(database:str, enable_cdf:bool=False)
```
Share all the tables in a database to the recipient. You can enable Change Data Feed (CDF) by setting enable_cdf to True.

```python
unshare_database(database:str)
```
Remove all the tables in a database from the share.

```python
share_table(table:str, enable_cdf:bool=False)
```
Share a table to the recipient. You can enable Change Data Feed (CDF) by setting enable_cdf to True.

```python
unshare_table(table:str)
```
Remove a table from the share.

```python
add_recipient(recipient:str, recipient_databricks_id:str="")
```
Add a recipient to the share. You can specify the Databricks workspace identifier for external workspaces.

```python
remove_recipient(recipient:str)
```
Remove a recipient from the share.

```python
drop_recipient(recipient:str)
```
Remove a recipient and all their permissions from the share.


### DeltaShareRecipient Class (data_recipient.py)
This is a Python class that provides various methods for working with Delta Sharing files. The class allows you to add share files, and perform operations such as syncing tables, refreshing the cache incrementally, and clearing sync history.

###Prerequisite
- delta-sharing

###Installation
```bash
pip install delta-sharing
```

###API Reference

```python
DeltaShareRecipient(share_file_loc:str, catalog:str="hive_metastore", table_prefix:str="")
```
This method initializes the class with the given parameters.
**share_file_loc (str)**: The path to the share file on dbfs or any other cloud storage location.
**catalog (str, optional)**: The catalog to use for the tables. Defaults to "hive_metastore".
**table_prefix (str, optional)**: The prefix to use for the tables. Defaults to "".

```python
discover() -> pyspark.sql.DataFrame
```
This method returns a DataFrame with all the information about the share file, including share, schema, and table.
Returns:
pyspark.sql.DataFrame: A dataframe containing the share, schema, and table.

```python
share_sync(cache_locally:bool=False, refresh_incrementally:bool=False, clear_previous_cache:bool=False, clear_sync_history:bool=False, primary_keys:dict=dict()) -> list
```
This method syncs all tables inside the share files.
**Args**
**cache_locally (bool, optional)**: Whether to cache the table locally. Defaults to False.
**refresh_incrementally (bool, optional)**: Whether to refresh the cache incrementally, note CDF must be enabled on the source table. Defaults to False.
**clear_previous_cache (bool, optional)**: Whether to clear the previous cache (warning: this will drop the tables and clear all content). Defaults to False.
**clear_sync_history (bool, optional)**: Whether to clear the sync history. Defaults to False.
**primary_keys (dict, optional)**: The primary keys for the the tables inside the share, this is needed for incremental updates to work. Defaults is empty {}, however you can pass it in this format {'table_x':'id1, id2, id3', 'table_y':'idx, idy'}.
**Returns**:
list: A list of sync ids.
```python
table_sync(share:str, source:str, target:str, primary_keys:str, cache_locally:bool=False, refresh_incrementally:bool=False, clear_previous_cache:bool=False) -> str
```
This method syncs a single table.

**Args**:
**share (str)**: The share name.
**source (str)**: The source schema and table name in this format 'schema.table'.
**target (str)**: The target schema and table name to which you want to save the source to, in this format 'schema.table'.
Rest of arguments are exactly as share_sync
**Returns**:
str: The sync id.



Authors

Amr Ali

License

This project is licensed under the MIT License.



