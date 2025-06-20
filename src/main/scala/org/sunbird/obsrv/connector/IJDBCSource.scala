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

  def batchQuery(table: String, timestampColumn: String, offset: Int, batchSize: Int, timestampOpt: Option[AnyRef], filterCondition: Option[String]): String = {
    val query = new StringBuilder
    query.append(s"SELECT * FROM $table")
    if (timestampOpt.isDefined || filterCondition.isDefined) {
      query.append(s" WHERE ")
    }
    val filters = scala.collection.mutable.ListBuffer[String]()
    timestampOpt.map(ts => filters += (s" $timestampColumn > '${ts.asInstanceOf[String]}'"))
    filterCondition.map { filter => filters += s" $filter"}

    query.append(filters.mkString(" AND "))

    val batchClause = s"ORDER BY $timestampColumn LIMIT $batchSize OFFSET $offset"

    println("Query: " + s"$query $batchClause")
    s"$query $batchClause"
  }

   def timeStampQuery(table: String, timestampColumn: String, timestamp: Any, filterCondition: Option[String] = None): String = {
    val query = new StringBuilder
    query.append(s"SELECT * FROM $table")
    if (filterCondition.isDefined || timestamp != null) {
      query.append(" WHERE ")
    }
    val filters = scala.collection.mutable.ListBuffer[String]()
    val timestampCondition = s"$timestampColumn = '${timestamp}'"
    filters += timestampCondition
    filterCondition.map { filter => filters += s"$filter"}

    query.append(filters.mkString(" AND "))
    println("Timestamp Query: " +  s"$query")
    query.toString()
  }

  def updateLastTimestamp(ctx: ConnectorContext, lastTimestamp: Any): Unit = {
    ctx.state.putState[String]("lastRecordTimestamp", s"${lastTimestamp}")
  }

}