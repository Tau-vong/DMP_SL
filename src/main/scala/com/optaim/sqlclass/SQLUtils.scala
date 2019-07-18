package com.optaim.sqlclass

/**
  * SQL
  */
object SQLUtils {
  val create_hive_db:String = """dmp"""
  val useDatabase:String = """use dmp"""
  val modeSet:String = """set hive.exec.dynamic.partition.mode=nonstrict"""
  val create_hive_table:String =
    """
      |CREATE TABLE IF NOT EXISTS original_table(
      |event_type STRING,
      |resource_type STRING,
      |coupon_id STRING,
      |product_id STRING,
      |source STRING,
      |jice_userid STRING,
      |mac STRING,
      |idfv STRING,
      |idfa STRING,
      |ip STRING,
      |region_id STRING,
      |city STRING,
      |province STRING,
      |country STRING,
      |time STRING,
      |net STRING,
      |operator STRING,
      |ap_name STRING,
      |ap_mac STRING,
      |os STRING,
      |osv STRING,
      |brand STRING,
      |model STRING,
      |cp STRING,
      |md STRING,
      |pl STRING,
      |ct STRING,
      |kw STRING,
      |sourcetype STRING,
      |shoulv_userid STRING,
      |user_mobile STRING,
      |user_birthday STRING,
      |user_gender STRING,
      |user_createtime STRING,
      |user_employee STRING,
      |user_source STRING,
      |product_name STRING,
      |businesstype STRING,
      |ent_id STRING,
      |ent_name STRING,
      |store_id STRING,
      |store_name STRING,
      |active_id STRING,
      |active_name STRING,
      |coupon_from STRING,
      |order_id STRING,
      |verification_id STRING,
      |resource_score STRING,
      |service_score STRING,
      |score STRING,
      |openid STRING,
      |user_type STRING,
      |wxid STRING,
      |order_count STRING,
      |order_amount STRING,
      |order_points STRING,
      |sale_price STRING,
      |sale_points STRING,
      |upload_points STRING,
      |upload_amount STRING,
      |actiontype STRING
      |）
      |PARTITIONED BY(
      |analysis_date DATE
      |）
    """.stripMargin

  val originalData:String=
    """insert into table original_table
      |select event_type,resource_type,
      |coupon_id,product_id,source,
      |jice_userid,mac,idfv,idfa,
      |ip,region_id,city,province,
      |country,time,net,operator,
      |ap_name,ap_mac,os,osv,
      |brand,model,cp,md,
      |pl,ct,kw,sourcetype,
      |shoulv_userid,user_mobile,
      |user_birthday,user_gender,
      |user_createtime,user_employee,
      |user_source,product_name,businesstype,
      |ent_id,ent_name,store_id,store_name,
      |active_id,active_name,coupon_from,order_id,
      |verification_id,resource_score,service_score,
      |score,openid,user_type,wxid,order_count,
      |order_amount,order_points,sale_price,
      |sale_points,upload_points,upload_amount,
      |actiontype,analysis_date
      |from original_data""".stripMargin
  val user_info:String =
    """
      |select
      |bigint(shoulv_userid),
      |bigint(user_mobile),
      |mac,
      |date(analysis_date),
      |int(businesstype),
      |case when event_type='Coupon_receive' then 1
      |     when event_type='Coupon_share' then 2
      |     when event_type='Ent_mark' then 3
      |     when event_type='Ent_view' then 4
      |     when event_type='Evaluate' then 5
      |     when event_type='Order' then 6
      |     when event_type='Product_share' then 7
      |     when event_type='Product_view' then 8
      |     when event_type='Product_mark' then 9
      |     when event_type='Store_mark' then 10
      |     when event_type='Store_view' then 11
      |     when event_type='Verification' then 12
      |     when event_type='Upload_points' then 13
      | else 0 end as event_type,
      |bigint(coupon_id),
      |case when province='北京市' then 2
      |     when province='内蒙古自治区' then 3
      |     when province='黑龙江省' then 4
      |     when province='山东省' then 5
      |     when province='河北省' then 6
      |     when province='安徽省' then 7
      |     when province='贵州省' then 8
      |     when province='辽宁省' then 9
      |     when province='重庆市' then 10
      |     when province='陕西省' then 11
      |     when province='天津市' then 12
      |     when province='江苏省' then 13
      |     when province='四川省' then 14
      |     when province='湖南省' then 15
      |     when province='广东省' then 16
      |     when province='云南省' then 17
      |     when province='江西省' then 18
      |     when province='宁夏回族自治区' then 19
      |     when province='河南省' then 20
      |     when province='浙江省' then 21
      |     when province='吉林省' then 22
      |     when province='湖北省' then 23
      |     when province='上海市' then 24
      |     when province='山西省' then 25
      |     when province='福建省' then 26
      |     when province='海南省' then 27
      |     when province='新疆维吾尔自治区'	 then 28
      |     when province='广西壮族自治区' then 29
      |     when province='甘肃省' then 30
      |     when province='西藏自治区' then 31
      |     when province='青海省' then 32
      | else 1 end as province,
      |user_gender,
      |case when user_source='online' then '1'
      | else user_source end as user_source,
      |bigint(user_birthday),
      |bigint(product_id),
      |bigint(ent_id),
      |bigint(store_id),
      |bigint(order_amount),
      |bigint(order_points),
      |bigint(sale_price),
      |bigint(sale_points),
      |int(order_count),
      |bigint(upload_points),
      |bigint(upload_amount)
      |from original_data
    """.stripMargin
  val dic_entid_entname:String =
    """
      |select distinct
      |bigint(ent_id) as ent_id,
      |ent_name
      |from original_table
      |where ent_id is not null and ent_name is not null
    """.stripMargin
  val dic_storeid_storename:String =
    """
      |select distinct
      |bigint(store_id) as store_id,
      |store_name
      |from original_table
      |where store_id is not null and store_name is not null
    """.stripMargin
  val dic_productid_productname:String =
    """
      |select distinct
      |bigint(product_id) as product_id,
      |product_name
      |from original_table
      |where product_id is not null and product_name is not null
    """.stripMargin
  val clear_ent_table:String =
    """
      |truncate table dic_entid_entname
    """.stripMargin
  val clear_store_table:String =
    """
      |truncate table dic_storeid_storename
    """.stripMargin
  val clear_product_table:String =
    """
      |truncate table dic_productid_productname
    """.stripMargin
  val update_data_offset:String=
    """
      |insert into user_update_data (user_pk_max)
      |select max(id) from user_info
    """.stripMargin

}
