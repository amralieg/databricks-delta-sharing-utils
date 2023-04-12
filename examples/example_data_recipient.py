# Databricks notebook source
# MAGIC %run ../python/data_recipient

# COMMAND ----------

# initialise data recipient class, provide one of share_profile_file_lo or provider_sharing_identifier, but not both at the same time
dsr = DeltaShareRecipient(share_profile_file_loc="<share-profile-location>", provider_sharing_identifier="<data-provider-sharing-identifier>", catalog="<catalog-name>")
#run discover to list all available shares to you, so you pick one of these share to use it in the next call
dsr.discover()

# COMMAND ----------

#start incremental sync of all tables inside the share 'amr_test_share_provider' which you discovered in the last step
dsr.create_incrementally_cached_tables(share="amr_test_share_provider", primary_keys = {'random_db.random_table':'id'})
