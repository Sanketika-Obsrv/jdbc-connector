package org.sunbird.obsrv.connector

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.sunbird.obsrv.connector.model.Models.ConnectorContext
import org.sunbird.obsrv.connector.source.{ISourceConnector, SourceConnector}

object JDBCConnector {

  def main(args: Array[String]): Unit = {
    SourceConnector.process(args, new JDBCSourceConnector)
  }
}

case class JDBCConfig(source: IJDBCSource, url: String, userName: String, userPwd: String, table: String, timestampColumn: String, batchSize: Int, numBatches: Int, filterCondition: Option[String] = None)

class JDBCSourceConnector extends ISourceConnector {

  override def getSparkConf(config: Config): Map[String, String] = {
    Map[String, String]()
  }

  override def process(spark: SparkSession, ctx: ConnectorContext, config: Config, metricFn: (String, Long) => Unit): Dataset[Row] = {
    val jdbcConfig = getJDBCConfig(config)
//    val recordsCount = countNewRecords(spark, ctx, jdbcConfig)
//    if (recordsCount > 0) {
    fetchRecords(spark, ctx, jdbcConfig, jdbcConfig.numBatches)
//    } else {
//      spark.emptyDataFrame
//    }
  }

  private def getDriver(dbType: String): IJDBCSource = {
    println(s"[JDBCSourceConnector] Getting driver for database type: $dbType")
    val driver = dbType match {
      case "postgresql" => new PostgresSource
      case "mysql" => new MySQLSource
      case "oracle" => new OracleSource
      case _ => throw new Exception(s"Unsupported database type: $dbType")
    }
    println(s"[JDBCSourceConnector] Driver created: ${driver.getClass.getSimpleName}")
    driver
  }

  private def fetchRecords(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig, batches: Int): DataFrame = {
    val dfList = for (idx <- 1 to batches) yield {
      fetchBatch(spark, ctx, jdbcConfig, (idx - 1) * jdbcConfig.batchSize)
    }
    val df = dfList.reduce((a, b) => a.union(b))
    val lastTimestamp: Any = df.agg(max(col(jdbcConfig.timestampColumn))).head().get(0)
    val lastTimestampDF = getAllTimestampRecords(spark, ctx, jdbcConfig, lastTimestamp)
    jdbcConfig.source.updateLastTimestamp(ctx, lastTimestamp)
    df.union(lastTimestampDF).distinct()
  }

  private def getJDBCConfig(config: Config): JDBCConfig = {
    val dbType = config.getString("source_database_type")
    val jdbcUrl = dbType match {
      case "oracle" =>
        s"jdbc:$dbType:thin:@//${config.getString("source_database_host")}:${config.getString("source_database_port")}/${config.getString("source_database_name")}"
      case "mysql" =>
        s"jdbc:$dbType://${config.getString("source_database_host")}:${config.getString("source_database_port")}/${config.getString("source_database_name")}"
      case "postgresql" =>
        s"jdbc:$dbType://${config.getString("source_database_host")}:${config.getString("source_database_port")}/${config.getString("source_database_name")}"
      case other =>
        throw new IllegalArgumentException(s"Unsupported database type: $other")
    }
    printf("Database connection url: " + jdbcUrl)
    
    // Read filter condition parameter
    val filterCondition = if (config.hasPath("source_table_filter_condition")) Some(config.getString("source_table_filter_condition")) else None
    
    JDBCConfig(
      source = getDriver(config.getString("source_database_type")), url = jdbcUrl,
      userName = config.getString("source_database_username"), userPwd = config.getString("source_database_pwd"),
      table = config.getString("source_table"), timestampColumn = config.getString("source_timestamp_column"),
      batchSize = config.getInt("source_batch_size"), numBatches = config.getInt("source_max_batches"),
      filterCondition = filterCondition
    )
  }

  private def readData(spark: SparkSession, jdbcConfig: JDBCConfig, query: String): DataFrame = {
    spark.read.format("jdbc")
      .option("driver", jdbcConfig.source.getDriver())
      .option("url", jdbcConfig.url)
      .option("user", jdbcConfig.userName)
      .option("password", jdbcConfig.userPwd)
      .option("query", query)
      .load()
  }

  private def countNewRecords(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig): Long = {
    val countQuery = jdbcConfig.source.countQuery(jdbcConfig.table, jdbcConfig.timestampColumn, ctx.state.getState[AnyRef]("lastRecordTimestamp"))
    val df = readData(spark, jdbcConfig, countQuery).select(col("count").cast("long").alias("count"))
    df.head().getAs[Long]("count")
  }

  private def fetchBatch(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig, offset: Int): DataFrame = {
    val selectQuery = jdbcConfig.source.batchQuery(jdbcConfig.table, jdbcConfig.timestampColumn, offset, jdbcConfig.batchSize, ctx.state.getState[AnyRef]("lastRecordTimestamp"), jdbcConfig.filterCondition)
    readData(spark, jdbcConfig, selectQuery)
  }

  private def getAllTimestampRecords(spark: SparkSession, ctx: ConnectorContext, jdbcConfig: JDBCConfig, lastTimestamp: Any): DataFrame = {
    val selectQuery = jdbcConfig.source.timeStampQuery(jdbcConfig.table, jdbcConfig.timestampColumn, lastTimestamp, jdbcConfig.filterCondition)
    readData(spark, jdbcConfig, selectQuery)
  }
}