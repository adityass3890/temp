package com.tmobile.dlmIngestion

import com.tmobile.common.{CommonMethods, SchemaDefinition}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.InputStreamReader
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

object stockonhandIngest {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(stockonhandIngest.getClass)

  def main(args: Array[String]): Unit = {

    //<-------------------------------------     Reading Config files -------------------------------------------------------------------------->>

    val hdfs: FileSystem = FileSystem.get(new URI(args(0)), new Configuration())
    val json_file = new InputStreamReader(hdfs.open(new Path(args(0) + args(1))))
    val source = args(2)
    val extractConfig = ConfigFactory.parseReader(json_file)

    CommonMethods.log4jLogGenerate(logger, extractConfig.getString("warehouse.logdir"), "stockonhand")
    val start_time = System.nanoTime
    val timeFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    val processed_starttime = timeFormat.format(cal.getTime).toString()

    logger.info("<----------- Stock on hand ingestion has started ---------------->")
    logger.info("Stock on hand - ingestion started on " + processed_starttime)

    val conf = new SparkConf(true).setAppName("stockonhand")
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
    val sourceTable = "stockonhand"
    val targetTable = "stockonhand"
    //val postDateFilter=args(3)
    val outboundPath = extractConfig.getString("warehouse.outbound_path") + "/" + sourceTable

    //<-------------------------------------     reading data from CSV files -------------------------------------------------------------------------->>

    var extractionDf = CommonMethods.readFromCSVFile(outboundPath + "/*part*", "|", spark, SchemaDefinition.stockonhandSchema)
    
    // Remove duplicates based on primary key (storeid, inventorystatus, skuid)
    extractionDf = extractionDf.dropDuplicates("storeid", "inventorystatus", "skuid")
    
    logger.info("Data loaded from CSV files, showing sample data:")
    extractionDf.show(false)
    extractionDf.printSchema()
    
    logger.info("Ingesting data to Oracle database")
    CommonMethods.saveToJDBC(extractionDf, targetTable, "append", target_env_enabled, extractConfig)
    
    cal = Calendar.getInstance()
    val processed_endtime = timeFormat.format(cal.getTime).toString()
    val end_time = System.nanoTime
    val duration = (end_time - start_time) / 1e9d
    
    logger.info("Stock on hand - ingestion completed on " + processed_endtime)
    logger.info(s"Total execution time: $duration seconds")
    logger.info("<----------- Stock on hand ingestion has completed ---------------->")
    
    spark.close()
    spark.stop()
  }

}