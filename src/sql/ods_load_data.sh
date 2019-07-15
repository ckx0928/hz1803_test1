#!/bin/bash
sqoop job --exec bap_biz_tradet
sqoop job --exec bap_cart
sqoop job --exec bap_code_category
sqoop job --exec bap_ods_user
sqoop job --exec bap_ods_user_addr
sqoop job --exec bap_ods_user_extend
sqoop job --exec bap_order_delivery
sqoop job --exec bap_order_item
sqoop job --exec bap_us_order
sqoop job --exec bap_user_app_click_log
sqoop job --exec bap_user_pc_click_log
