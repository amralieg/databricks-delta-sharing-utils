# DeltaShareProvider
The __init__ method initializes a DeltaShareProvider instance with the share name, recipient email address, and optional recipient_databricks_id and drop_if_exists arguments. If drop_if_exists is True, then the method first drops the recipient and the share if they already exist. Then it creates a share with the given share name if it doesn't already exist, and adds the recipient to the share.

The drop_share method drops the share with the share name if it exists.

The share_catalog method shares all databases in the given catalog to the share with the share name. If enable_cdf is True, it also enables Change Data Feed (CDF) for each shared table. The method skips sharing the information_schema and default databases.

The unshare_catalog method unshares all databases in the given catalog from the share with the share name. The method skips unsharing the information_schema and default databases.

The share_database method shares all tables in the given database to the share with the share name. If enable_cdf is True, it also enables Change Data Feed (CDF) for each shared table.

The unshare_database method unshares all tables in the given database from the share with the share name.

The share_table method shares the given table to the share with the share name. If enable_cdf is True, it enables Change Data Feed (CDF) for the table. If the table is already shared, the method updates the share to include the table's history.

The unshare_table method unshares the given table from the share with the share name.

The add_recipient method adds the given recipient to the share with the share name. If a recipient_databricks_id is provided, the method creates a Databricks recipient using the sharing identifier provided. The method grants the recipient SELECT access to the share. If the recipient_databricks_id is not provided, the method provides instructions to the recipient on how to create an open recipient.

The remove_recipient method removes the given recipient from the share with the share name.

The drop_recipient method drops the given recipient if it exists.

The __spark_sql method executes a SQL statement using the Apache Spark SQL API.

The __log method prints the given thing with an "[info]" prefix.




