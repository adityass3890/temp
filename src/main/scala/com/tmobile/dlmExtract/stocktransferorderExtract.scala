package com.tmobile.dlmExtract

import com.tmobile.common.CommonMethods
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.InputStreamReader
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

object stocktransferorderExtract {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(stocktransferorderExtract.getClass)

  def main(args: Array[String]): Unit = {

    //<-------------------------------------     Reading Config files -------------------------------------------------------------------------->>

    val hdfs: FileSystem = FileSystem.get(new URI(args(0)), new Configuration())
    val json_file = new InputStreamReader(hdfs.open(new Path(args(0) + args(1))))
    val source = args(2)
    val extractConfig = ConfigFactory.parseReader(json_file)

    CommonMethods.log4jLogGenerate(logger, extractConfig.getString("warehouse.logdir"), "stocktransferorder")
    val start_time = System.nanoTime
    val timeFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    val processed_starttime = timeFormat.format(cal.getTime).toString()

    logger.info("<----------- Stock transfer order extraction has started ---------------->")
    logger.info("Stock transfer order - extraction started on " + processed_starttime)

    val conf = new SparkConf(true).setAppName("stocktransferorder")
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
    var extractionDf: org.apache.spark.sql.DataFrame = null
    val sourceTable = "stocktransferorder"
    val targetTable = "stocktransferorder"
    val outboundPath = extractConfig.getString("warehouse.outbound_path") + "/" + sourceTable

    //<-------------------------------------     reading data from Cassandra -------------------------------------------------------------------------->>
    val cols = "*"
    // Reading entire table without date filtering as per requirements
    val extractionQuery = "select " + cols + " from mycatalog." + keyspace + "." + sourceTable
    
    logger.info("Extraction query: " + extractionQuery)
    
    //Reading the data from sourceTable Cassandra Table
    extractionDf = CommonMethods.readCassandraTableQuery(sourceTable, source_env_enabled, extractConfig, extractionQuery)
    
    // Handle complex list/frozen type conversions to comma-separated strings for Oracle compatibility
    
    // Transform deliveryorderlist from list<frozen<fdeliveryorder>> to string
    extractionDf = extractionDf.withColumn("deliveryorderlist", 
      when(col("deliveryorderlist").isNull, lit(null))
      .otherwise(regexp_replace(
        regexp_replace(col("deliveryorderlist").cast("string"), "^\\[", ""),
        "\\]$", ""
      ))
    )
    
    // Transform nonserializedmateriallist from list<frozen<fnonserializedmaterial>> to string
    extractionDf = extractionDf.withColumn("nonserializedmateriallist", 
      when(col("nonserializedmateriallist").isNull, lit(null))
      .otherwise(regexp_replace(
        regexp_replace(col("nonserializedmateriallist").cast("string"), "^\\[", ""),
        "\\]$", ""
      ))
    )
    
    // Transform serializedmateriallist from list<frozen<fserializedmaterial>> to string
    extractionDf = extractionDf.withColumn("serializedmateriallist", 
      when(col("serializedmateriallist").isNull, lit(null))
      .otherwise(regexp_replace(
        regexp_replace(col("serializedmateriallist").cast("string"), "^\\[", ""),
        "\\]$", ""
      ))
    )
    
    // Transform stolineitem from list<frozen<fstolineitem>> to string
    extractionDf = extractionDf.withColumn("stolineitem", 
      when(col("stolineitem").isNull, lit(null))
      .otherwise(regexp_replace(
        regexp_replace(col("stolineitem").cast("string"), "^\\[", ""),
        "\\]$", ""
      ))
    )
    
    // Transform trackingnumberlist from list<text> to string
    extractionDf = extractionDf.withColumn("trackingnumberlist", 
      when(col("trackingnumberlist").isNull, lit(null))
      .otherwise(regexp_replace(
        regexp_replace(col("trackingnumberlist").cast("string"), "^\\[", ""),
        "\\]$", ""
      ))
    )
    
    logger.info("Data extraction completed, writing to CSV file")
    
    CommonMethods.writeToCSVFile(extractionDf, "|", outboundPath, "overwrite")
    
    cal = Calendar.getInstance()
    val processed_endtime = timeFormat.format(cal.getTime).toString()
    val end_time = System.nanoTime
    val duration = (end_time - start_time) / 1e9d
    
    logger.info("Stock transfer order - extraction completed on " + processed_endtime)
    logger.info(s"Total execution time: $duration seconds")
    logger.info("<----------- Stock transfer order extraction has completed ---------------->")
    
    spark.close()
    spark.stop()
  }

}