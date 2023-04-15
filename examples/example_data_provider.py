# Databricks notebook source
# MAGIC %run ../python/data_provider

# COMMAND ----------

# MAGIC %sql
# MAGIC drop catalog if exists __deltasharing_test_975__ cascade;
# MAGIC create catalog __deltasharing_test_975__;
# MAGIC create database __deltasharing_test_975__.__delta_share_test975__;
# MAGIC create table __deltasharing_test_975__.__delta_share_test975__.table1 (id int, name string);
# MAGIC insert into __deltasharing_test_975__.__delta_share_test975__.table1 values (1, "a"), (2, "b"), (3, "c");

# COMMAND ----------

# create a DeltaShareProvider instance for a share 'my_share' and to be shared with a recipient 'my_recipient'
# after running this code, you will get an activation link to be shared with your recipient to download the share file
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient", drop_share_if_exists=True, drop_recipient_if_exists=True)

# share a table with Change Data Feed enabled so the data recipient can incrementally load the data
dsp.share_table(table="__deltasharing_test_975__.__delta_share_test975__.table1", enable_cdf=True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC update __deltasharing_test_975__.__delta_share_test975__.table1 set name="d" where id=3;
# MAGIC delete from __deltasharing_test_975__.__delta_share_test975__.table1 where id=1;
# MAGIC insert into __deltasharing_test_975__.__delta_share_test975__.table1 values (4, "d"), (5, "e"), (6, "f");
