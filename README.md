
# Delta Share Utils

Delta Sharing utils includes set of python files that makes it very easy to sync tables shared using Delta Sharing protocol.

## How it works

These utilities are orhanised in 2 main python files, data_provider.py and data_recipient.py

## DeltaShareProvider
A python class that provides a simplified way to share Delta tables and databases between different Databricks workspaces. The class allows you to share catalogs, databases, and tables to a recipient by using a Databricks share.

### Prerequisites
This class is built to be used in a Databricks environment. Ensure that you have access to a Databricks workspace, and you are familiar with Python, SQL, and Delta.

### Installation
You can copy the entire code and use it in your Databricks notebook or import the class to your codebase.

### Usage
The class allows you to share catalogs, databases, and tables to a recipient using a Databricks share. You can use the following methods to perform the actions.

```python
DeltaShareProvider(share:str, recipient:str, recipient_databricks_id:str="",        drop_if_exists:bool=False)
```
Create an instance of the class with the following parameters:
**share**: The name of the Databricks share.
**recipient**: The name of the recipient who will receive the shared data.
**recipient_databricks_id**: (optional) The identifier of the Databricks workspace where the recipient resides. Required when sharing with external Databricks workspaces.
**drop_if_exists**: (optional) Set to True to delete the recipient and the share if they already exist.

### API Reference

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

### Examples
```python
# create a DeltaShareProvider instance
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient")

# share all the tables in a database
dsp.share_database(database="my_database")

# share a table with Change Data Feed enabled
dsp.share_table(table="my_database.my_table", enable_cdf=True)

# add a recipient
dsp.add_recipient(recipient="my_friend")

# remove a recipient
dsp.remove_recipient(recipient="my_friend")
```
Authors
Amr Ali
License
This project is licensed under the MIT License - see the LICENSE file for details.



