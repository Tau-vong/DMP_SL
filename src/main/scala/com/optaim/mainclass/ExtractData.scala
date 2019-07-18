package com.optaim.mainclass

import java.io.File
import java.sql.DriverManager
import java.util.Properties

import com.optaim.sqlclass.SQLUtils
import org.apache.spark.sql.SparkSession

/**
  * Extract user_info/store_dic/product_dic/ent_dic data
  */
object ExtractData {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |请输入参数
          |   InputPath
        """.stripMargin)
    }
    val inputPath = args(0)

    val warehouseLocation = new File("spark-warehouse")
      .getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName(s"DealWithData")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.read.format("CSV")
      .option("header","true")
      .load(inputPath)
      .createOrReplaceTempView("original_data")

    import spark.sql
    sql(SQLUtils.create_hive_db)
    sql(SQLUtils.useDatabase)
    sql(SQLUtils.modeSet)
    sql(SQLUtils.create_hive_table)

    sql(SQLUtils.originalData)
    val df = sql(SQLUtils.user_info)
    val df1 = sql(SQLUtils.dic_entid_entname)
    val df2 = sql(SQLUtils.dic_storeid_storename)
    val df3 = sql(SQLUtils.dic_productid_productname)

    val prop=new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("mysql.properties")
    prop.load(in)
    prop.getProperty("user")
    prop.getProperty("password")
    prop.getProperty("driver")
    prop.getProperty("charset")

    val url = prop.getProperty("db.url")
    val t1 = prop.getProperty("table.t1")
    val t2 = prop.getProperty("table.t2")
    val t3 = prop.getProperty("table.t3")
    val t4 = prop.getProperty("table.t4")

    Class.forName("com.mysql.jdbc.Driver")
    val conn=DriverManager.getConnection(url,prop)
    val sm1=conn.prepareCall(SQLUtils.clear_ent_table)
    val sm2=conn.prepareCall(SQLUtils.clear_store_table)
    val sm3=conn.prepareCall(SQLUtils.clear_product_table)
    sm1.execute()
    sm1.close()
    sm2.execute()
    sm2.close()
    sm3.execute()
    sm3.close()

    df.write.mode("append").jdbc(url,t1,prop)
    df1.write.mode("append").jdbc(url,t2,prop)
    df2.write.mode("append").jdbc(url,t3,prop)
    df3.write.mode("append").jdbc(url,t4,prop)
    val sm4=conn.prepareCall(SQLUtils.update_data_offset)
    sm4.execute()
    sm4.close()
    spark.stop()

  }
}
