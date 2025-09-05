package com.tmobile.common

import com.tmobile.dlmExtract.dualimeiinfobyprimaryimeiExtract.logger

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

import java.sql.DriverManager
import java.util.Properties
import java.util.Base64

object CommonMethods {

  //<-------------------------------------    Common Methods      -------------------------------------------------------------------------->>
  //Copy or rename file in hdfs location
  def copyContents(hdfsSrcPath: String, hdfsTgtPath: String, SrcFileName: String, hdfsTgtFileName: String, opsType: String): Unit = {
    val spark = SparkSession.getActiveSession.get;
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration);
    val opType: String = opsType.toLowerCase()
    opType match {
      case "copy" =>
        val hdfsSrcFileName = fileSystem.globStatus(new Path(hdfsSrcPath + SrcFileName))(0).getPath().getName();
        org.apache.hadoop.fs.FileUtil.copy(fileSystem, new Path(hdfsSrcPath + hdfsSrcFileName), fileSystem, new Path(hdfsTgtPath + hdfsTgtFileName), false, true, new Configuration)
      case "rename" =>
        val hdfsSrcFileName = fileSystem.globStatus(new Path(hdfsSrcPath + SrcFileName))(0).getPath().getName();
        fileSystem.rename(new Path(hdfsTgtPath + hdfsSrcFileName), new Path(hdfsTgtPath + hdfsTgtFileName));
      case _ =>
    }
  }

  //Read CSV file
  def readFromCSVFile(path: String, delimiter: String, spark: SparkSession, schema: StructType): DataFrame = {
    import org.apache.spark.sql.functions._;
    import spark.implicits._;
    var dataFrame: org.apache.spark.sql.DataFrame = null
    Try {
      if (delimiter.length == 1) {
        dataFrame = spark.read.
          option("header", "true").
          option("delimiter", delimiter).
          option("quote", "\"").
          schema(schema).
          option("ignoreLeadingWhiteSpace", "true").
          option("ignoreTrailingWhiteSpace", "true").
          option("inferSchema", "true").
          option("compression","gzip").
          csv(path)
      }
      else {
        var delimFormattedString = "\\"
        delimFormattedString += delimiter.mkString("\\")
        val rdd = spark.sparkContext.textFile(path)
        val columnHeader = rdd.first()
        val filteredRdd = rdd.filter(line => !line.equals(columnHeader))
        val columnHeadings = columnHeader.split(delimFormattedString)
        dataFrame = filteredRdd.map(line => line.split(delimFormattedString)).toDF.select((0 until columnHeadings.length).map(i => col("value").getItem(i).as(columnHeadings(i))): _*)
      }
    } match {
      case Success(obj) => dataFrame
      case Failure(obj) => {
        println(obj.getMessage() + " : " + obj.getCause())
        throw obj;
      }
    }
  }

  //Reading Cassandra table
  def readCassandraTable(table: String, env: String, config: com.typesafe.config.Config): DataFrame = {
    val spark = SparkSession.getActiveSession.get;
    var cass_env = config.getString("environment")
    var df: org.apache.spark.sql.DataFrame = null
    Try {
      val decodeBytes=Base64.getDecoder.decode(config.getString(s"cassandra_source.$env.password"))
      val decodePW= new String(decodeBytes)
      logger.info("decodePW is "+decodePW)
      spark.conf.set("spark.cassandra.connection.host", config.getString(s"cassandra_source.$env.hostname"))
      spark.conf.set("spark.cassandra.connection.port", config.getString(s"cassandra_source.$env.port"))
      spark.conf.set("spark.cassandra.auth.username", config.getString(s"cassandra_source.$env.username"))
      spark.conf.set("spark.cassandra.auth.password", decodePW)
      spark.conf.set("spark.cassandra.connection.ssl.enabled", "true")
      spark.conf.set("spark.cassandra.connection.ssl.trustStore.path", config.getString(s"cassandra_source.$env.truststorepath"))
      spark.conf.set("spark.cassandra.connection.ssl.trustStore.password", config.getString(s"cassandra_source.$env.truststorepassword"))
      df = spark.read.cassandraFormat(table, config.getString(s"cassandra_source.$env.keyspace"), config.getString(s"cassandra_source.$env.cluster_name"), true)
        .option("spark.sql.dse.search.enableOptimization", "On").load()

    } match {
      case Success(obj) => df
      case Failure(obj) => {
        println(obj.getMessage() + " : " + obj.getCause())
        throw obj;
      }
    }
    return df;
  }

  //Read cassandra table by passing query
  def readCassandraTableQuery(table: String, env: String, config: com.typesafe.config.Config, query: String): DataFrame = {
    val spark = SparkSession.getActiveSession.get;
    var cass_env = config.getString("environment")
    var df: org.apache.spark.sql.DataFrame = null
    Try {
      val decodeBytes=Base64.getDecoder.decode(config.getString(s"cassandra_source.$env.password"))
      val decodePW= new String(decodeBytes)
      import com.datastax.spark.connector.cql._
      spark.conf.set("spark.cassandra.connection.host", config.getString(s"cassandra_source.$env.hostname"))
      spark.conf.set("spark.cassandra.connection.port", config.getString(s"cassandra_source.$env.port"))
      spark.conf.set("spark.cassandra.auth.username", config.getString(s"cassandra_source.$env.username"))
      spark.conf.set("spark.cassandra.auth.password", decodePW)
      spark.conf.set("spark.cassandra.connection.ssl.enabled", "true")
      spark.conf.set("spark.cassandra.connection.ssl.trustStore.path", config.getString(s"cassandra_source.$env.truststorepath"))
      spark.conf.set("spark.cassandra.connection.ssl.trustStore.password", config.getString(s"cassandra_source.$env.truststorepassword"))
      spark.conf.set("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
      //spark.conf.set("spark.cassandra.input.split.size", "1048576")
      df = spark.sql(query)
      //read.cassandraFormat(table, config.getString(s"cassandra_source.$env.keyspace"), config.getString(s"cassandra_source.$env.cluster_name"), true)
      // .option("spark.sql.dse.search.enableOptimization", "On").load()

    } match {
      case Success(obj) => df
      case Failure(obj) => {
        println(obj.getMessage() + " : " + obj.getCause())
        throw obj;
      }
    }
    return df;
  }
  def saveToParquet(dataFrameToSave: DataFrame, mode: String, path: String) {
    val spark = SparkSession.getActiveSession.get;
    spark.conf.set("spark.sql.parquet.filterPushdown", "true")
    spark.conf.set("spark.sql.parquet.int96TimestampConversion", "true")
    Try {
      dataFrameToSave.repartition(200).write.mode(mode).format("parquet").option("compression","snappy").save(path)
    } match {
      case Success(obj) => Unit
      case Failure(obj) => {
        println(obj.getMessage() + " : " + obj.getCause())
        throw obj;
      }
    }
  }

  //Write dataframe to CSV file
  def writeToCSVFile(dataFrameToSave: DataFrame, delimiter: String, path: String, savemode: String) {
    Try {
      dataFrameToSave.repartition(120).write.mode(savemode)
        .format("csv")
        .option("delimiter", delimiter)
        .option("header", "true")
        .option("quote", "")
        .option("escape", "")
        .option("compression","gzip")
        .save(path)
    } match {
      case Success(obj) => Unit
      case Failure(obj) => {
        println(obj.getMessage() + " : " + obj.getCause())
        throw obj;
      }
    }
  }

  //Write dataframe to Oracle
  def saveToJDBC(dataFrameToSave: DataFrame, dbtable: String, savemode: String, env: String, config: com.typesafe.config.Config) {
    import java.sql.DriverManager
    val oracleDecodeBytes=Base64.getDecoder.decode(config.getString(s"oracle_target.$env.password"))
    val oracleDecodePW= new String(oracleDecodeBytes)
    val connection = DriverManager.getConnection(config.getString(s"oracle_target.$env.url"), config.getString(s"oracle_target.$env.username"), oracleDecodePW)
    Try {
      if (connection.isClosed()) {
        dataFrameToSave.repartition(100).write.format("jdbc").
          option("url", config.getString(s"oracle_target.$env.url")).
          option("driver", config.getString(s"oracle_target.$env.driver")).
          option("user", config.getString(s"oracle_target.$env.username")).
          option("password", oracleDecodePW).
          option("truncate", "true").
          option("header", "true").
          option("batchsize", "10000").
          option("dbtable", config.getString(s"oracle_target.$env.schema")+"."+dbtable).
          mode(savemode).
          save()
        connection.close()
      }
      else {
        connection.close()
        dataFrameToSave.repartition(100).write.format("jdbc").
          option("url", config.getString(s"oracle_target.$env.url")).
          option("driver", config.getString(s"oracle_target.$env.driver")).
          option("user", config.getString(s"oracle_target.$env.username")).
          option("password", oracleDecodePW).
          option("truncate", "true").
          option("header", "true").
          option("batchsize", "10000").
          option("dbtable", config.getString(s"oracle_target.$env.schema")+"."+dbtable).
          mode(savemode).
          save()
        connection.close()

      }
    } match {
      case Success(obj) => Unit
      case Failure(obj) => {
        println(obj.getMessage() + " : " + obj.getCause())
        throw obj;
      }
    }
  }
//Save jdbc upsert
//Read Oracle table
def saveJDBC(env: String, config: com.typesafe.config.Config, mergeSql: String) {
  val connectionProperties = new Properties();
  connectionProperties.put("user", config.getString(s"oracle_target.$env.username"))
  connectionProperties.put("password", config.getString(s"oracle_target.$env.password"))
  connectionProperties.put("driver", config.getString(s"oracle_target.$env.driver"))
  val connection = DriverManager.getConnection(config.getString(s"oracle_target.$env.url"), connectionProperties)
  val statement=connection.createStatement()
  statement.execute(mergeSql)
  connection.close()
}
  //Read Oracle table
  def extractJDBC(spark: SparkSession, table: String, env: String, config: com.typesafe.config.Config, query: String): DataFrame = {
    val connectionProperties = new Properties();
    connectionProperties.put("user", config.getString(s"oracle_target.$env.username"))
    connectionProperties.put("password", config.getString(s"oracle_target.$env.password"))
    connectionProperties.put("driver", config.getString(s"oracle_target.$env.driver"))
    val inputDataFrame = spark.read.jdbc(config.getString(s"oracle_target.$env.url"),"( "+query+" )",connectionProperties)
    inputDataFrame
  }
  def extractJDBC(spark: SparkSession, table: String, env: String, config: com.typesafe.config.Config): DataFrame = {
    val inputDataFrame = spark.read.format("jdbc")
      .option("url", config.getString(s"oracle_target.$env.url"))
      .option("user", config.getString(s"oracle_target.$env.username"))
      .option("password", config.getString(s"oracle_target.$env.password"))
      .option("dbtable", table)
      .option("driver", config.getString(s"oracle_target.$env.driver")).load();
    inputDataFrame
  }
  //Creating Custom logger
  def log4jLogGenerate(logger: Logger, path: String, logfile: String) {
    import org.apache.log4j._
    val layout = new SimpleLayout();
    val appender = new RollingFileAppender(layout, path + logfile + ".log", true);
    appender.setLayout(new PatternLayout("[%p] %d - %m%n"));
    appender.setMaxFileSize("2MB");
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
  }

  def mappingApiNames(inputDataFrame: DataFrame, map: Map[String, String]): DataFrame = {
    var returnDataFrame=inputDataFrame
    map.foreach {
      case (key, value) => {
        returnDataFrame=returnDataFrame.withColumn("outboundapiname",when(lower(col("outboundapiname")).contains(key.toLowerCase),lit(value)).otherwise(col("outboundapiname")))
      }
    }
    returnDataFrame
  }
}