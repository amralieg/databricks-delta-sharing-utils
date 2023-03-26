# delta-share-utils
Delta Sharing utils includes set of python files that makes it very easy to sync tables shared using Delta Sharing protocol

Delta Share Recipient
Delta Share Recipient is a Python module that simplifies working with Delta Sharing files.

Installation
python
Copy code
!pip install delta-sharing
Usage
python
Copy code
import delta_sharing

# Instantiate DeltaShareRecipient
d = DeltaShareRecipient(share_file_loc, catalog, table_prefix)

# Get all tables in the share
d.discover()

# Sync a whole share
d.share_sync()

# Sync a single table
d.table_sync(share, source, target, primary_keys, cache_locally, refresh_incrementally, clear_previous_cache)
DeltaShareRecipient
DeltaShareRecipient allows you to add a share file and perform various operations that make it easy to work with Delta Share files.

Constructor
python
Copy code
def __init__(self, share_file_loc:str, catalog:str="hive_metastore", table_prefix:str=""):
Arguments
share_file_loc: The location of the share file.
catalog: The name of the catalog to use. Default is "hive_metastore".
table_prefix: The prefix to add to the table name. Default is an empty string.
discover()
python
Copy code
def discover(self):
Returns a dataframe with all the information about the share file, including share, schema, and table.

share_sync()
python
Copy code
def share_sync(self, cache_locally:bool=False, refresh_incrementally:bool=False, clear_previous_cache:bool=False, clear_sync_history:bool=False, primary_keys:dict=dict()) -> list:
Syncs a whole share.

Arguments
cache_locally: Whether to cache the table locally. Default is False.
refresh_incrementally: Whether to refresh the cache incrementally. Default is False.
clear_previous_cache: Whether to clear the previous cache. Default is False.
clear_sync_history: Whether to clear the sync history. Default is False.
primary_keys: A dictionary of primary keys for the tables to be synced. Default is an empty dictionary.
table_sync()
python
Copy code
def table_sync(self, share:str, source:str, target:str, primary_keys:str, cache_locally:bool=False, refresh_incrementally:bool=False, clear_previous_cache:bool=False) -> str:
Syncs a single table.

Arguments
share: The name of the share.
source: The name of the source table.
target: The name of the target table.
primary_keys: A dictionary of primary keys for the table to be synced. Default is an empty dictionary.
cache_locally: Whether to cache the table locally. Default is False.
refresh_incrementally: Whether to refresh the cache incrementally. Default is False.
clear_previous_cache: Whether to clear the previous cache. Default is False.
Requirements
Python 3.6 or higher.
Delta Sharing Python module (delta-sharing).
Apache Spark.