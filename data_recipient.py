import delta_sharing
import uuid
import re
import sys

class DeltaShareRecipient:
  """
  This class allows you to add a share files, and perform various operations that make it easy to work with delta share files.
  """
  def __init__(self, share_file_loc:str, catalog:str="hive_metastore", table_prefix:str=""):
    """
    Constructor method to initialize the class with the given parameters.

    Args:
        share_file_loc (str): The path to the share file on dbfs or any other cloud storage location.
        catalog (str, optional): The catalog to use for the tables. Defaults to "hive_metastore".
        table_prefix (str, optional): The prefix to use for the tables. Defaults to "".
    """
    self.share_file_loc = share_file_loc
    self.table_prefix = table_prefix
    self.catalog = catalog
    self.deltasharing_client = delta_sharing.SharingClient(share_file_loc.replace("dbfs:", "/dbfs/"))
    self.sync_runs_db = 'delta_share_sync'
    self.sync_runs_table = f"{self.sync_runs_db}.{self.table_prefix}sync_runs"
    #get current user
    self.current_user= self.__spark_sql("select current_user() as user;").collect()[0][0]

  def discover(self):
    """
    Returns a dataframe with all the information about the share file, including share, schema, and table.

    Returns:
        pyspark.sql.DataFrame: A dataframe containing the share, schema, and table.
    """
    return spark.createDataFrame(data=self.deltasharing_client.list_all_tables(), schema = ["table","schema","share"]).select("share","schema","table")
  
  def __clear_local_cache(self, cache_locally:bool=False, refresh_incrementally:bool=False, clear_sync_history:bool=False):
    self.__spark_sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.sync_runs_db};")
    if clear_sync_history:
      if refresh_incrementally:
        self.__log("[info] clear_sync_history is ignored since refresh_incrementally is set to True")
      else:
        self.__spark_sql(f"DROP TABLE IF EXISTS {self.catalog}.{self.sync_runs_table};")

  def share_sync(self, cache_locally:bool=False, refresh_incrementally:bool=False,\
                 clear_previous_cache:bool=False, clear_sync_history:bool=False, primary_keys:dict=dict())->list:
    """
    This method will sync all tables inside the share files.

    Args:
        cache_locally (bool, optional): Whether to cache the table locally. Defaults to False.
        refresh_incrementally (bool, optional): Whether to refresh the cache incrementally, note CDF must be enabled on the source table. Defaults to False.
        clear_previous_cache (bool, optional): Whether to clear the previous cache (warning: this will drop the tables and clear all content). Defaults to False.
        clear_sync_history (bool, optional): Whether to clear the sync history. Defaults to False.
        primary_keys (dict, optional): The primary keys for the the tables inside the share, this is needed for incremental updates to work. Defaults is empty {},\
        however you can pass it in this format {'table_x':'id1, id2, id3', 'table_y':'idx, idy'}.

    Returns:
        list: A list of sync ids.
    """
    sync_ids = list()
    self.__clear_local_cache(cache_locally, clear_previous_cache, clear_sync_history)
    for table in self.deltasharing_client.list_all_tables():
      sync_id = self.table_sync(table.share, f"{table.schema}.{table.name}", f"{self.catalog}.{table.schema}.{self.table_prefix}{table.name}",\
                                primary_keys.get(f"{table.schema}.{table.name}"), cache_locally, refresh_incrementally, clear_previous_cache)
      sync_ids.append(sync_id)
    self.summerise(sync_ids)
    return sync_ids

  def table_sync(self, share:str, source:str, target:str, primary_keys:str, cache_locally:bool=False,\
                 refresh_incrementally:bool=False, clear_previous_cache:bool=False)->str:
    """
    This method will sync a single table.

    Args:
        share (str): The share name.
        source (str): The source schema and table name in this format 'schema.table'.
        target (str): The target schema and table name to which you want to save the source to, in this format 'schema.table'.
        primary_keys (dict, optional): The primary keys for the the tables inside the share, this is needed for incremental updates to work. Defaults is empty {},\
        however you can pass it in this format {'table_x':'id1, id2, id3', 'table_y':'idx, idy'}.
        cache_locally (bool, optional): Whether to cache the table locally. Defaults to False.
        refresh_incrementally (bool, optional): Whether to refresh the cache incrementally, note CDF must be enabled on the source table. Defaults to False.
        clear_previous_cache (bool, optional): Whether to clear the previous cache (warning: this will drop the tables and clear all content). Defaults to False.
    Returns:
        str: The sync id.
    """
    sync_id=None
    if cache_locally == False:
      sync_type = "remotely-stored"
    elif refresh_incrementally == True:
      sync_type = "locally-cashed-incrementally-refreshed"
    else:
      sync_type = "locally-cashed-fully-refreshed"
    if cache_locally==False and refresh_incrementally:
      raise Exception("Can not have refresh_incrementally=True while table is cache_locally=False, set refresh_incrementally=False and try again.")
    try:
      for table in self.deltasharing_client.list_all_tables():
        if table.share == share and f"{table.schema}.{table.name}" == source:
          self.__spark_sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{table.schema};")
          break
              
      if clear_previous_cache:
        if refresh_incrementally:
          self.__log("[info] clear_previous_cache is ignored since refresh_incrementally is set to True")
        else:
          self.__spark_sql(f"DROP TABLE IF EXISTS {target};")

      status = 'STARTED'
      message = "table_sync started"
      starting_from_version = -1
      #insert initial entry recrod to log this run where
      sync_id=self.__log_table_sync_event(sync_id, share, source, starting_from_version, target, sync_type,\
                                          status, message,'null', 'null', 'null', 'null')
      self.__create_table(sync_id, share, source,target, primary_keys, sync_type)
    except Exception as e:
      status = "FAILED"
      message = "table_sync failed, exception message: " + str(e).replace('\'','"')
      self.__log(message)
      sync_id=self.__log_table_sync_event(sync_id, share, source, starting_from_version, target, sync_type,\
                                          status, message,'null', 'null', 'null', 'null')
      raise e
    return sync_id

  def __create_table(self, sync_id:str, share:str, source:str, target:str, primary_keys:str, sync_type:str):
    file_loc = self.share_file_loc.replace("/dbfs", "dbfs:")
    table_url = file_loc + f"#{share}.{source}" #must be in driver node fs
    (last_sync_type, starting_from_version, last_completion_timestamp) = self.__get_last_sync_version(source)
    max_table_version = self.__get_max_table_version(table_url, source)
    self.__log(f"table {source} max version is {max_table_version}")
    if sync_type == "remotely-stored":
      if spark.catalog.tableExists(target) == False:
        self.__spark_sql(f"""CREATE TABLE {target}
                     USING deltaSharing
                     LOCATION '{file_loc}#{share}.{source}';""")

        status = "SUCCESS"
        message = "table created as remote table"
        num_affected_rows = "null"
        num_updated_rows="null"
        num_deleted_rows="null"
        num_inserted_rows="null"
        starting_from_version = 'null'
      else:
        status = "FAILED"
        message = f'Target table {target} already exists. drop it manually before continuing or set clear_previous_cache=True.\
        Note setting this option will drop this table "{target}" and clear all contect'
        num_affected_rows = "null"
        num_updated_rows="null"
        num_deleted_rows="null"
        num_inserted_rows="null"
        starting_from_version = 'null'
    #create local copy if 
    # 1- sync_type = locally-cashed-fully-refreshed
    # 2- sync_type = locally-cashed-incrementally-refreshedor but tables does not exists
    # 3- sync_type = locally-cashed-incrementally-refreshedor but no previous sync version
    # 4- sync_type = locally-cashed-incrementally-refreshedor and last sync version is greater than max table version (seems table is deleted and
    # recreated at source)
    elif ((sync_type == "locally-cashed-fully-refreshed") or 
    (sync_type == "locally-cashed-incrementally-refreshed" and spark.catalog.tableExists(f"{target}") == False) or
    (sync_type == "locally-cashed-incrementally-refreshed" and starting_from_version is None) or
    (sync_type == "locally-cashed-incrementally-refreshed" and isinstance(max_table_version, str)) or
    (sync_type == "locally-cashed-incrementally-refreshed" and starting_from_version > max_table_version)):
      self.__log("starting locally-cashed-fully-refreshed")
      delta_sharing.load_as_spark(table_url).write.saveAsTable(target, format="delta", mode="overwrite")
      status = "SUCCESS"
      message = "table created as fully refreshed local table"
      if sync_type == "locally-cashed-incrementally-refreshed":
        starting_from_version = max_table_version
        message = "table created as incrementally refreshed local table"
      else:
        starting_from_version = 'null'
      num_affected_rows = self.__get_table_count(target)
      num_updated_rows="null"
      num_deleted_rows="null"
      num_inserted_rows=num_affected_rows
    elif sync_type == "locally-cashed-incrementally-refreshed":
      self.__log("starting locally-cashed-incrementally-refreshed")
      if starting_from_version == max_table_version:
        status = "SUCCESS"
        message = f"update skipped, table {target} is up to date and last sync version is {starting_from_version}"
        self.__log(message)
        num_affected_rows = 0
        num_updated_rows=0
        num_deleted_rows=0
        num_inserted_rows=0
      else:
        if starting_from_version<=0 or starting_from_version > max_table_version:
          starting_from_version = max_table_version
        (status, message, starting_from_version, num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows) = \
        self.__sync_incrementally(sync_id, share, table_url, source, starting_from_version, max_table_version, target, primary_keys, last_sync_type,\
                                  last_completion_timestamp)
    else:
      message = f"Unknow sync type specified for table {source}"
      Exception(message)
    self.__log_table_sync_event(sync_id, share, source, starting_from_version, target, sync_type, status,\
                             message, num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows)

  def __sync_incrementally(self, sync_id:str, share:str, table_url, source:str, starting_from_version, max_table_version, target:str, primary_keys:str,\
                           last_sync_type, last_completion_timestamp):  
    
    if primary_keys is None or primary_keys.strip()=="":
      raise Exception("can not perform incremental refresh without specifying table primary keys. you can pass primary keys as dictionary,\
                      for example {'table_x':'pk1, pk2', 'table_y':'pk3, pk4'}")
    
    conditions = [f"__source.{c.strip()}=__target.{c.strip()}" for c in primary_keys.split(',')]
    on_conditions = " and ".join([str(con) for con in conditions])
    
    view_name = f'{source}_temp_view'.replace(".","_")
    #create temp view with table changes
    delta_sharing.load_table_changes_as_spark(url=table_url, starting_version=starting_from_version).createOrReplaceTempView(view_name)
    #delete all records that match the update_postimage ids

    self.__spark_sql(f'create or replace temp view partitioned_{view_name} as \
    (select * from (SELECT *, rank() over (partition by {primary_keys} order by _commit_version desc) as __rank FROM {view_name}) \
    where _change_type != "update_preimage" and __rank=1);')
    
    result = self.__spark_sql(f"merge into {target} as __target using partitioned_{view_name} as __source on {on_conditions} \
    when matched and __source._change_type in('delete','update_preimage') then delete\
    when matched and __source._change_type in ('insert', 'update_postimage') then update set *\
    when not matched and __source._change_type in ('insert', 'update_postimage') then insert *;")\
    .collect();
    
    num_affected_rows = result[0][0]
    num_updated_rows = result[0][1]
    num_deleted_rows = result[0][2]
    num_inserted_rows = result[0][3]
    latest_version = self.__spark_sql(f'select max(_commit_version) from {view_name};').collect()[0][0]
    self.__spark_sql(f'drop view {view_name};')
    self.__spark_sql(f'drop view partitioned_{view_name};')
    return ("SUCCESS", "table incrementally updated successfully", latest_version,\
            num_affected_rows, num_updated_rows, num_deleted_rows, num_inserted_rows)
  
  def __get_last_sync_version(self, source:str):
    last_version_df = self.__spark_sql(f"select sync_type, source_last_sync_version, completion_time from {self.sync_runs_table}\
    where source_table='{source}' and status='SUCCESS' and source_last_sync_version = (select max(source_last_sync_version)\
    from {self.sync_runs_table} where source_table='{source}' and status='SUCCESS' and source_last_sync_version is not null)\
    order by completion_time desc limit 1;")
    if last_version_df.isEmpty() == False:
      return (last_version_df.collect()[0][0], last_version_df.collect()[0][1], last_version_df.collect()[0][2])
    else:
      return (None, None, None)
  
  def __get_max_table_version(self, table_url:str, source:str)->int:
    try:
      delta_sharing.load_table_changes_as_spark(url=table_url, starting_version=sys.maxsize).count()
    except Exception as e: #as expected the previous statement failed, lets extract the max table version from the error message
      pattern = r"latest version of the table\((\d+)\)"
      match = re.search(pattern, str(e))
      if match:
        version = int(match.group(1))
      else:
        version = 'null'
    return version
  
  def __log_table_sync_event(self, sync_id:str, share:str, source:str, starting_from_version:int, target:str, sync_type:str,\
                             status:str, message:str, num_affected_rows, num_updated_rows,num_deleted_rows, num_inserted_rows):
    if spark.catalog.tableExists(self.sync_runs_table) == False:
      #create sync_log table if not exists
      self.__spark_sql(f"create table {self.sync_runs_table} (sync_id string, share string, source_table string, source_last_sync_version int,\
      target_table string, started_by string, started_time timestamp, completion_time timestamp, status string, sync_type string,\
      num_affected_rows int, num_updated_rows int, num_deleted_rows int, num_inserted_rows int, message string);")
    if sync_id is None:
      sync_id = str(uuid.uuid4())
      self.__spark_sql(f"""insert into {self.sync_runs_table} values('{sync_id}', '{share}', '{source}', null, '{target}', '{self.current_user}',\
      current_timestamp(), null, '{status}', '{sync_type}', null, null, null, null, '{message}');""")
    else:
      self.__spark_sql(f"update {self.sync_runs_table} set source_last_sync_version={starting_from_version}, completion_time = current_timestamp(),\
      status='{status}', sync_type='{sync_type}', num_affected_rows={num_affected_rows}, num_updated_rows = {num_updated_rows},\
      num_deleted_rows = {num_deleted_rows}, num_inserted_rows={num_inserted_rows}, message='{message}' where sync_id='{sync_id}';")
    return sync_id

  def __get_table_count(self, target):
      return self.__spark_sql(f"select count(*) from {target};").collect()[0][0]

  def __spark_sql(self, sql):
    self.__log(sql)
    return spark.sql(sql)

  def __log(self, thing):
    #print(thing)
    #print()
    pass
  
  def summerise(self, sync_ids:list):
    """
    This method will display the summary of the sync operations performed.

    Args:
        sync_ids (list): A list of sync ids.
    """
    syncs = (', '.join('"' + sync_id + '"' for sync_id in sync_ids))
    display(self.__spark_sql(f"select status, source_table, target_table, num_affected_rows,\
    (unix_timestamp(completion_time)-unix_timestamp(started_time)) as duration_seconds,\
    message from {self.sync_runs_table} where sync_id in ({syncs}) order by duration_seconds desc;"))