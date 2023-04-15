# Databricks notebook source
# MAGIC %run ../python/data_recipient

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Share Recipient Example
# MAGIC You must run this on a Single Node Cluster with Single User
# MAGIC Before you run this example, make sure you create catalog _open_datasets_ using this command:
# MAGIC ```CREATE CATALOG _open_datasets_;```
# MAGIC if you do not have create catalog permission, use catalog hive_metastore

# COMMAND ----------

# initialise data recipient class, with an open datasets share profile.
dsr = DeltaShareRecipient(share_profile_file_loc="https://databricks-datasets-oregon.s3-us-west-2.amazonaws.com/delta-sharing/share/open-datasets.share", catalog="hive_metastore", table_prefix="deltashare_")
#run discover to list all available shares to you, so you pick one of these share to use it in the next call
dsr.discover()

# COMMAND ----------

#start incremental sync of all tables inside the share 'amr_test_share_provider' which you discovered in the last step
dsr.create_remotely_linked_tables(share="delta_sharing")

# COMMAND ----------

# MAGIC %sql select * from hive_metastore.default.deltashare_COVID_19_NYT limit 10;
