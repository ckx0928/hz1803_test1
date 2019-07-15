#!/bin/bash
echo "create full table starting...."
hive -f hive_ods_create_table.sql
hive -f hive_dwd_create_table.sql
hive -f hive_dws_create_table.sql
echo "create full table ended...."