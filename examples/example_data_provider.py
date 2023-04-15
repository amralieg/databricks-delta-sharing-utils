# Databricks notebook source
# MAGIC %run ../python/data_provider

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists hive_metastore.__delta_share_test975_ cascade;
# MAGIC create database hive_metastore.__delta_share_test975_;
# MAGIC create table hive_metastore.__delta_share_test975_.table1 (id int, name string);
# MAGIC insert into hive_metastore.__delta_share_test975_.table1 values (1, "a"), (2, "b"), (3, "c");

# COMMAND ----------

# create a DeltaShareProvider instance for a share 'my_share' and to be shared with a recipient 'my_recipient'
# after running this code, you will get an activation link to be shared with your recipient to download the share file
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient", drop_share_if_exists=True, drop_recipient_if_exists=True)

# share a table with Change Data Feed enabled so the data recipient can incrementally load the data
dsp.share_table(table="hive_metastore.__delta_share_test975_.table1", enable_cdf=True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC update hive_metastore.__delta_share_test975_.table1 set name="d" where id=3;
# MAGIC delete from hive_metastore.__delta_share_test975_.table1 where id=1;
# MAGIC insert into hive_metastore.__delta_share_test975_.table1 values (4, "d"), (5, "e"), (6, "f");

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER SHARE my_share ADD TABLE hive_metastore.__delta_share_test975_.table1 WITH HISTORY;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe catalog hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC describe share my_share;
