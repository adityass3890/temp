package com.tmobile.dlmIngestion

import com.tmobile.common.{CommonMethods, SchemaDefinition}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

import java.io.InputStreamReader
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

object serializedDeviceHistoryCassToOracle {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(serializedDeviceHistoryCassToOracle.getClass)

  def main(args: Array[String]): Unit = {

    //<-------------------------------------     Reading Config files -------------------------------------------------------------------------->>

    val hdfs: FileSystem = FileSystem.get(new URI(args(0)), new Configuration())
    val json_file = new InputStreamReader(hdfs.open(new Path(args(0) + args(1))))
    val source = args(2)
    val extractConfig = ConfigFactory.parseReader(json_file)

    CommonMethods.log4jLogGenerate(logger, extractConfig.getString("warehouse.logdir"), "serializedDeviceHistoryCassToOracle")
    val start_time = System.nanoTime
    val timeFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    val processed_starttime = timeFormat.format(cal.getTime).toString()

    logger.info("<----------- Outbound transaction extraction has started ---------------->")
    logger.info("Outbound transaction - extraction started on " + processed_starttime)

    val conf = new SparkConf(true).setAppName("serializedDeviceHistoryCassToOracle")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark.conf.set("spark.sql.parquet.int96AsTimestamp", "true")
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    spark.conf.set("spark.shuffle.encryption.enabled", "true")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration);
    //Source Environment Check

    val source_env_enabled = source + "." + extractConfig.getString("sourceEnabled")
    val target_env_enabled = extractConfig.getString("targetEnabled")
    val keyspace = extractConfig.getString("cassandra_source." + source_env_enabled + ".keyspace")
    val sourceTable = "serializeddevicehistory_v2"
    val targetTable = "serial_device_hist"
    val postDateFilter=args(3)

    val outboundPath = extractConfig.getString("warehouse.outbound_path") + "/" + sourceTable

    //<-------------------------------------     reading data from Cassandra -------------------------------------------------------------------------->>
    val cols = "serialnumber,transactionid,ban,bringyourowndeviceindicator,carrierdetails,channel,claimnumber,company,conditionofdevice,deliverydate,destinationpoint,dolineitemid,donumber,eipdetails,enddate,eventname,externalreference,filedate,filename,filetime,imsi,inventorymovementtype,leaseloanind,manufacturer,mastercartonid,materialdetails,meiddecimal,meidhexadecimal,model,offerstatus,offerupdateddate,ordercreateddate,orderlineid,ordertime,originalserial,originatingsystem,owningstore,palletid,polineitem,posnrdetails,postingdate,postingtime,programcode,purchaseorder,quotenumber,quoteprice,refurbishedindicator,rejectionreason,retaillineitem,retailtradeinserial,returnorderlineitem,rmaexpirationdate,rmanumber,salesorderlineitem,secondaryimei,serialtype,skuidfrom,sourcecreationdate,sourcepoint,sprintfinancialaccountnumber,sprintnetworkactivationdate,storagelocation,subscribernumber,systemreference,trackingnumber,transactionstatus,transactiontype,vendorcode"
    //val cols ="*"
    var extractionQuery = ""
    if(postDateFilter.contains("NO")) {
      extractionQuery = "select "+cols+" from mycatalog." + keyspace + "." + sourceTable
    } else {
      logger.info("posting dates " +extractConfig.getString("tables."+sourceTable+".postingdateStart"))
      extractionQuery = "select " + cols + " from mycatalog." + keyspace + "." + sourceTable +" where postingdate between '"+extractConfig.getString("tables."+sourceTable+".postingdateStart")+"' and '"+extractConfig.getString("tables."+sourceTable+".postingdateEnd")+"'"
    }
    //Reading the data from sourceTable Cassandra Table
    var extractionDf = CommonMethods.readCassandraTableQuery(sourceTable, source_env_enabled, extractConfig, extractionQuery)
   // var extractionDf = CommonMethods.readFromCSVFile(outboundPath + "/*part*", "|", spark, SchemaDefinition.dualImeiPrimaryImeiSchema)
    extractionDf=extractionDf
      .withColumn("transactionid",when(col("transactionid").isNull," ").otherwise(col("transactionid")))
      .withColumn("postingtime",when(col("postingtime").isNull,99999).otherwise(col("postingtime")))
      .withColumn("postingdate",when(col("postingdate").isNull,"9999-01-01").otherwise(col("postingdate")))
    extractionDf=extractionDf.withColumn("postingdate",col("postingdate").cast("Date"))
      .withColumn("deliverydate",col("deliverydate").cast("Date"))
      .withColumn("enddate",col("enddate").cast("Date"))
      .withColumn("filedate",col("filedate").cast("Date"))
      .withColumn("offerupdateddate",col("offerupdateddate").cast("Date"))
      .withColumn("ordercreateddate",col("ordercreateddate").cast("Date"))
      .withColumn("rmaexpirationdate",col("rmaexpirationdate").cast("Date"))
      .withColumn("sourcecreationdate",col("sourcecreationdate").cast("Date"))
      .withColumn("sprintnetworkactivationdate",col("sprintnetworkactivationdate").cast("Date"))
      .withColumn("filetime",col("filetime").cast("Integer"))
      .withColumn("ordertime",col("ordertime").cast("Integer"))
      .withColumn("postingtime",col("postingtime").cast("Integer"))
      .withColumn("quotenumber",col("quotenumber").cast("Integer"))
      .withColumn("quoteprice",col("quoteprice").cast("Integer"))

    extractionDf=extractionDf.dropDuplicates("postingdate","postingtime","serialnumber","transactionid")

    CommonMethods.saveToJDBC(extractionDf, targetTable, "append", target_env_enabled, extractConfig)
    spark.close()
    spark.stop()
  }

}