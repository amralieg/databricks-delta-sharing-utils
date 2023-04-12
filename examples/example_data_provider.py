# Databricks notebook source
# MAGIC %run ../python/data_provider

# COMMAND ----------

# create a DeltaShareProvider instance for a share 'my_share' and to be shared with a recipient 'my_recipient'
# after running this code, you will get an activation link to be shared with your recipient to download the share file
dsp = DeltaShareProvider(share="my_share", recipient="my_recipient")

# share a table with Change Data Feed enabled so the data recipient can incrementally load the data
dsp.share_table(table="my_database.my_table", enable_cdf=True)
