
# Delta Share Utils

Delta Sharing utils includes set of python files that makes it very easy to sync tables shared using Delta Sharing protocol on Databricks platform.

## How it works

These utilities are organised in 2 main notebooks, python/data_provider.ipynb and python/data_recipient.ipynb.
to use this code, follow these steps:

1- At the data provider side:

 A- clone this git repo to the data provider workspace
 
 B- open the examples/data_provider notebook and use it to add create shares
 
2- At the data recipient side:

 A- clone this git repo to the data recipient workspace
 
 B- open the examples/data_recipient notebook and use it to sync shares

#### Examples for DeltaShareProvider Class (to run by the Data Provider)
```python
# create a DeltaShareProvider instance for a share 'my_share' and to be shared with a recipient 'my_recipient'
# after running this code, you will get an activation link to be shared with your recipient to download the share file
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient")

# share all the tables in a database with my recipients
dsp.share_database(database="my_database")

# share a table with Change Data Feed enabled so the data recipient can incrementally load the data
dsp.share_table(table="my_database.my_table", enable_cdf=True)
```

#### Examples for DeltaShareRecipient Class (to run by the Data Consumer)
```python
# create a DeltaShareRecipient instance and point it to the share file location that was downloaded form the activation link
dsr = DeltaShareRecipient('/dbfs/FileStore/tables/amr_azure_share.share')

# this will display a list of all shares, and what tables are shared
dsr.discover()

# this will start sync the data from the data sharer to the data recipients incrementally:
dsr.create_incrementally_cached_tables("my_share", primary_keys={'table1':'key1, key2'})-
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
DeltaShareProvider(share:str, recipient:str, recipient_sharing_identifier:str="", drop_if_exists:bool=False)
```
Create an instance of the class with the following parameters:

**share**: The name of the Databricks share.

**recipient**: The name of the recipient who will receive the shared data.

**recipient_sharing_identifier**: (optional) The identifier of the Databricks workspace where the recipient resides. Required when sharing with external Databricks workspaces.

**drop_if_exists**: (optional) Set to True to delete the recipient and the share if they already exist before creating them again.

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
add_recipient(recipient:str, recipient_sharing_identifier:str="")
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

### Prerequisite
- delta-sharing

### Installation
```bash
pip install delta-sharing
```

### API Reference

```python
DeltaShareRecipient(share_profile_file_loc:str="", provider_sharing_identifier:str="", catalog:str="hive_metastore", table_prefix:str="")
```
This method initializes the class with the given parameters.

**share_profile_file_loc (str)**: The path to the share file on dbfs or any other cloud storage location, if you provided this, do not provide provider_sharing_identifier.

**provider_sharing_identifier (str)**: Databricks sharing identifier of the provider of the data, if you provided this, do not provide share_profile_file_loc.

**catalog (str, optional)**: The catalog to use for the tables. Defaults to "hive_metastore", mandatory if you provided provider_sharing_identifier.

**table_prefix (str, optional)**: The prefix to use for the tables. Defaults to "".

```python
discover() -> pyspark.sql.DataFrame
```
This method displays a DataFrame with all the information about the share provided.

**Returns**:

pyspark.sql.DataFrame: A dataframe containing the share, schema, and table.

```python
 def create_remotely_linked_tables(self, share:str)->list:
```
This will mount all delta share tables as mirrored table of the source, no data copy is performed, and all queries will be run against the source directly (note this will incurre egress cost each run at the source).

**Args**

**share (str)**: share name that contains the tables to be mounted.

**Returns**:

list: A list of sync ids.

```python
 def create_fully_cached_tables(self, share:str)->list:
```
This will create all delta share tables locally as managed tables, and all tables will be exact copy of the source. you should run this method periodically to keep the cached copy up to date with the source. (note this will incurre egress cost each run at the source)

**Args**

**share (str)**: share name that contains the tables to be mounted.

**Returns**:

list: A list of sync ids.

```python
 def create_incrementally_cached_tables(self, share:str)->list:
```
This will create all delta share tables locally as managed tables, and all tables will be exact copy of the source at the first run, however subsequent runs will only sync the table changes form source (Note, this assumes that CDF has been turned on at source, use the data_prodiver notebook to do that). you should run this method periodically to keep the cached copy up to date with the source. (note this will incurre egress cost each run at the source, however for only the changed data)

**Args**

**share (str)**: share name that contains the tables to be mounted.

**primary_keys (dict, optional)**: The primary keys for the the tables inside the share, this is needed for incremental updates to work. Defaults is empty {}, however you can pass it in this format {'table_x':'id1, id2, id3', 'table_y':'idx, idy'}.

**Returns**:

list: A list of sync ids.

### Limitations
In some scenarios merging data at target may fail if the CDF contained conflicting changes at the same version, for example an insert immediatly followed by delete.

### Legal Information
This software is provided as-is and is not officially supported by Databricks through customer technical support channels. Support, questions, and feature requests can be communicated through the Issues page of this repo. Issues with the use of this code will not be answered or investigated by Databricks Support.


#### Authors

[Amr Ali](https://www.linkedin.com/in/amralieg/)

License

This project is licensed under the MIT License.



