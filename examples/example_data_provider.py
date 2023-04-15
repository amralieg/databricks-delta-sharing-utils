# Databricks notebook source
# MAGIC %run ../python/data_provider

# COMMAND ----------

catalog = "__deltasharing_test_975__"
spark.sql(f"drop catalog if exists {catalog} cascade");
spark.sql(f"create catalog {catalog}");
spark.sql(f"create database {catalog}.__delta_share_test975__");
spark.sql(f"create table {catalog}.__delta_share_test975__.table1 (id int, name string)");
spark.sql(f"insert into {catalog}.__delta_share_test975__.table1 values (1, 'a'), (2, 'b'), (3, 'c')");

# COMMAND ----------

# create a DeltaShareProvider instance for a share 'my_share' and to be shared with a recipient 'my_recipient'
# after running this code, you will get an activation link to be shared with your recipient to download the share file
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient", drop_share_if_exists=True, drop_recipient_if_exists=True)

# share a table with Change Data Feed enabled so the data recipient can incrementally load the data
dsp.share_table(table=f"{catalog}.__delta_share_test975__.table1", enable_cdf=True)

# COMMAND ----------

spark.sql(f"update {catalog}.__delta_share_test975__.table1 set name='d' where id=3;")
spark.sql(f"delete from {catalog}.__delta_share_test975__.table1 where id=1;")
spark.sql(f"insert into {catalog}.__delta_share_test975__.table1 values (4, 'd'), (5, 'e'), (6, 'f');")
