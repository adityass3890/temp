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

object dualimeiinfobyprimaryimeiIngest {

  @transient lazy val logger = org.apache.log4j.LogManager.getLogger(dualimeiinfobyprimaryimeiIngest.getClass)

  def main(args: Array[String]): Unit = {

    //<-------------------------------------     Reading Config files -------------------------------------------------------------------------->>

    val hdfs: FileSystem = FileSystem.get(new URI(args(0)), new Configuration())
    val json_file = new InputStreamReader(hdfs.open(new Path(args(0) + args(1))))
    val source = args(2)
    val extractConfig = ConfigFactory.parseReader(json_file)

    CommonMethods.log4jLogGenerate(logger, extractConfig.getString("warehouse.logdir"), "dualimeiinfobyprimaryimeiIngest")
    val start_time = System.nanoTime
    val timeFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    var cal = Calendar.getInstance()
    val processed_starttime = timeFormat.format(cal.getTime).toString()

    logger.info("<----------- Outbound transaction extraction has started ---------------->")
    logger.info("Outbound transaction - extraction started on " + processed_starttime)

    val conf = new SparkConf(true).setAppName("dualimeiinfobyprimaryimeiIngest")
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
    val sourceTable = "dualimeiinfo_by_primary_imei"
    val targetTable = "dualimeiinfo_primaryimei"
    //val postDateFilter=args(3)
    val outboundPath = extractConfig.getString("warehouse.outbound_path") + "/" + sourceTable

    //<-------------------------------------     reading data from Cassandra -------------------------------------------------------------------------->>

    var extractionDf = CommonMethods.readFromCSVFile(outboundPath + "/*part*", "|", spark, SchemaDefinition.dualImeiPrimaryImeiSchema)
    extractionDf=extractionDf.dropDuplicates("SERIALNUM")
    CommonMethods.saveToJDBC(extractionDf, targetTable, "append", target_env_enabled, extractConfig)
    spark.close()
    spark.stop()
  }

}