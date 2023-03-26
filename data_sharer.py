# Databricks notebook source
class DeltaShareProvider:
  def __init__(share:str):
    self.share = share
    self.tables = dict()
    self.recipients = set()
  
  def add_table(self, table:str, enable_cdf:bool=False, enable_history:bool=False):
    tables.add(table, (self.share, table, enable_cdf, enable_history))
    return self
  
  def remove_table(self, table:str):
    tables.remove(table)
    return self
  
  def add_recipient(self, recipient:str):
    recipients.add(recipient)
    return self
  
  def remove_recipient(self, table:str):
    recipients.remove(recipient)
    return self
  
  def share(self):
    
    
  def __spark_sql(self, sql):
    self.__log(sql)
    return spark.sql(sql)

  def __log(self, thing):
    print(thing)
    print()
    pass
  
    

# COMMAND ----------

# MAGIC %sql
# MAGIC create share amr_test_share;

# COMMAND ----------


