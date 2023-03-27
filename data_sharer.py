# Databricks notebook source
class DeltaShareProvider:
  def __init__(self, share:str, recipient:str, recipient_databricks_id:str="", drop_if_exists:bool=False):
    """
    Initializes a DeltaShareProvider instance with the given parameters.

    Args:
        share (str): The name of the Delta share to use.
        recipient (str): The name of the recipient to share the Delta share with.
        recipient_databricks_id (str): The Databricks ID of the recipient. Defaults to an empty string.
        drop_if_exists (bool): Whether to drop the recipient and the share if they already exist. Defaults to False.
    """
    self.share = share
    if drop_if_exists:
      self.drop_recipient(recipient)
      self.drop_share()
    self.__spark_sql(f"CREATE SHARE IF NOT EXISTS {share};")
    self.add_recipient(recipient, recipient_databricks_id)
    
  def drop_share(self):
    """
    Drops the Delta share if it exists.
    """
    self.__spark_sql(f"DROP SHARE IF EXISTS {self.share};")
  
  def share_catalog(self, catalog:str, enable_cdf:bool=False):
    """
    Shares all databases in the specified catalog to the Delta share.

    Args:
        catalog (str): The name of the catalog to share.
        enable_cdf (bool): Whether to enable change data feed (CDF) on the shared tables. Defaults to False.

    Returns:
        DeltaShareProvider: The instance of DeltaShareProvider.
    """
    databases = self.__get_database_objects("databases", catalog, 'databaseName')
    self.__log(f'sharing all databases in catalog {catalog} to share {self.share}')
    for database in databases:
      if database == "information_schema" or databases == "default":
        self.__log(f"skipping sharing database {catalog}.{database}")
        continue
      self.share_database(f"{catalog}.{database}", enable_cdf)
    self.__log(f'all databased in catalog {catalog} shared in share {self.share}')
    return self
  
  def unshare_catalog(self, catalog:str):
    """
    Removes all databases in the specified catalog from the Delta share.

    Args:
        catalog (str): The name of the catalog to remove from the Delta share.

    Returns:
        DeltaShareProvider: The instance of DeltaShareProvider.
    """
    databases = self.__get_database_objects("databases", catalog, 'databaseName')
    self.__log(f'unsharing all databases in catalog {catalog} from share {self.share}')
    for database in databases:
      if database == "information_schema" or databases == "default":
        continue
      self.unshare_database(f"{catalog}.{database}")
    self.__log(f'catalog {catalog} compeletly unshared from share {self.share}')
    return self
  
  def share_database(self, database:str, enable_cdf:bool=False):
    """
    Shares all tables in the specified database to the Delta share.

    Args:
        database (str): The name of the database to share.
        enable_cdf (bool): Whether to enable change data feed (CDF) on the shared tables. Defaults to False.

    Returns:
        DeltaShareProvider: The instance of DeltaShareProvider.
    """
    tables = self.__get_database_objects("tables", database, 'tableName')
    self.__log(f'sharing all tables in database {database} to share {self.share}')
    for table in tables:
      self.share_table(f"{database}.{table}", enable_cdf)
    self.__log(f'all tables in database {database} shared in share {self.share}')
    return self
  
  def unshare_database(self, database:str):
    """
    Unshares all tables in the specified database from the share.
    
    Args:
        database (str): Name of the database.
    
    Returns:
        self
    """
    tables = self.__get_database_objects("tables", database, 'tableName')
    self.__log(f'unsharing all tables in database {database} from share {self.share}')
    for table in tables:
      self.unshare_table(f"{database}.{table}")
    self.__log(f'database {database} compeletly unshared from share {self.share}')
    return self
  
  def share_table(self, table:str, enable_cdf:bool=False):
    """
    Shares the specified table to the share.
    
    Args:
        table (str): Name of the table.
        enable_cdf (bool): Whether to enable Change Data Feed for the shared table (default False).
    
    Returns:
        self
    """
    try:
      if enable_cdf:
        self.unshare_table(table) #unshare it first to enable cdf
        self.__spark_sql(f'ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true);')
        self.__spark_sql(f'ALTER SHARE {self.share} ADD TABLE {table} WITH HISTORY;')
        self.__log(f'table {table} added to share {self.share} with CDF and History turned on')
      else:
        self.__spark_sql(f'ALTER SHARE {self.share} ADD TABLE {table};')
        self.__log(f'table {table} added to share {self.share}')
    except Exception as e:
      self.__log(str(e))
    return self
  
  def unshare_table(self, table:str):
    """
    Unshares the specified table from the share.
    
    Args:
        table (str): Name of the table.
    
    Returns:
        self
    """
    try:
      self.__spark_sql(f'ALTER SHARE {self.share} REMOVE TABLE {table};')
      self.__log(f'table {table} removed form share {self.share}')
    except Exception as e:
      self.__log(str(e))
    return self
  
  def add_recipient(self, recipient:str, recipient_databricks_id:str=""):
    """
    Adds a recipient to the share and grants SELECT access to them.
    
    Args:
        recipient (str): Name of the recipient.
        recipient_databricks_id (str): ID of the Databricks instance where the recipient is located (optional).
    
    Returns:
        self
    """
    try:
      if recipient_databricks_id is None or recipient_databricks_id.strip()=="":
        self.__log(f'open recipient {recipient} will be created. open the activation link provided in the table displayed below (scroll to the end), \
        and share it with the recipient.')
        display(self.__spark_sql(f'create recipient if not exists {recipient};'))
      else:
        display(self.__spark_sql(f'create recipient if not exists {recipient} using "{recipient_databricks_id}";'))
        self.__log(f'databirkcs recipient {recipient} created using the sharing identifier provided. inform the recipient so they can start reading the shares.')
      
      self.__spark_sql(f'GRANT SELECT on SHARE {self.share} TO RECIPIENT {recipient};')
      self.__log(f'recipient {recipient} granted SELECT on share {self.share}')
    except Exception as e:
      self.__log(str(e))
    return self
  
  def remove_recipient(self, recipient:str):
    """
    Removes SELECT access to the specified recipient from the share.
    
    Args:
        recipient (str): Name of the recipient.
    
    Returns:
        self
    """
    try:
      self.__spark_sql(f'REVOKE SELECT ON SHARE {self.share} FROM RECIPIENT {recipient};')
      self.__log(f'SELECT access on share {self.share} is revoked from  recipient {recipient}')
    except Exception as e:
      self.__log(str(e))
    return self
  
  def drop_recipient(self, recipient:str):
    """
    Drops the specified recipient.
    
    Args:
        recipient (str): Name of the recipient.
    
    Returns:
        self
    """
    try:
      self.__spark_sql(f'DROP RECIPIENT IF EXISTS {recipient};')
      self.__log(f'recipient {recipient} is dropped')
    except Exception as e:
      self.__log(str(e))
    return self
    
  def __spark_sql(self, sql):
    #print(sql)
    return spark.sql(sql)

  def __log(self, thing):
    print("[info] " + thing)
    pass
  
  def __get_database_objects(self, object_type, source, selector):
    return list(self.__spark_sql(f"show {object_type} in {source};").toPandas()[selector])

# COMMAND ----------

dsp = DeltaShareProvider(share="amr_test_share_provider", recipient="amr_aws_Account", drop_if_exists=True)
dsp.share_catalog("amrali_cat", enable_cdf=True)
dsp.unshare_catalog("amrali_cat")
