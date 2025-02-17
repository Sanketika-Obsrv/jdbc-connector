package org.sunbird.obsrv.connector

import org.sunbird.obsrv.connector.model.Models.ConnectorContext

trait IJDBCSource {

  def getDriver(): String

   def countQuery(table: String, timestampColumn: String, timestampOpt: Option[AnyRef]): String = {
    timestampOpt.map(timestamp => {
      s"SELECT COUNT(*) as count FROM $table WHERE $timestampColumn > '${timestamp.asInstanceOf[String]}'"
    }).orElse(
      Some(s"SELECT COUNT(*) as count FROM $table")
    ).get
  }

  def batchQuery(table: String, timestampColumn: String, offset: Int, batchSize: Int, timestampOpt: Option[AnyRef]): String = {
    timestampOpt.map(timestamp => {
      s"SELECT * FROM $table WHERE $timestampColumn > '${timestamp.asInstanceOf[String]}' ORDER BY $timestampColumn LIMIT $batchSize OFFSET $offset"
    }).orElse(
      Some(s"SELECT * FROM $table ORDER BY $timestampColumn LIMIT $batchSize OFFSET $offset")
    ).get
  }

   def timeStampQuery(table: String, timestampColumn: String, timestamp: Any): String = {
    s"SELECT * FROM $table WHERE $timestampColumn = '${timestamp}'"
  }

 def updateLastTimestamp(ctx: ConnectorContext, lastTimestamp: Any): Unit = {
    ctx.state.putState[String]("lastRecordTimestamp", s"${lastTimestamp}")
  }

}