package org.sunbird.obsrv.connector

class OracleSource extends IJDBCSource {

  override def getDriver(): String = "oracle.jdbc.driver.OracleDriver"

  override def countQuery(table: String, timestampColumn: String, timestampOpt: Option[AnyRef]): String = {
    timestampOpt.map(timestamp => {
      s"""SELECT COUNT(*) as count FROM $table WHERE s"$timestampColumn = TO_TIMESTAMP('${timestamp}', 'YYYY-MM-DD HH24:MI:SS.FF9')"""
    }).orElse(
      Some(s"SELECT COUNT(*) as count FROM $table")
    ).get
  }

  override def timeStampQuery(table: String, timestampColumn: String, timestamp: Any, filterCondition: Option[String]): String = {
    val query = new StringBuilder
    query.append(s"SELECT * FROM $table")
    if (filterCondition.isDefined || timestamp != null) {
      query.append(" WHERE ")
    }
    val filters = scala.collection.mutable.ListBuffer[String]()
    val timestampCondition = s"$timestampColumn = TO_TIMESTAMP('${timestamp}', 'YYYY-MM-DD HH24:MI:SS.FF9')"
    filters += timestampCondition
    filterCondition.map { filter => filters += s"$filter"}

    query.append(filters.mkString(" AND "))
    query.toString()
  }

  override def batchQuery(table: String, timestampColumn: String, offset: Int, batchSize: Int, timestampOpt: Option[AnyRef], filterCondition: Option[String]): String = {
    val query = new StringBuilder
    query.append(s"SELECT * FROM $table")
    if (timestampOpt.isDefined || filterCondition.isDefined) {
      query.append(s" WHERE ")
    }
    val filters = scala.collection.mutable.ListBuffer[String]()
    timestampOpt.map(ts => filters += s"$timestampColumn = TO_TIMESTAMP($ts, 'YYYY-MM-DD HH24:MI:SS.FF9')")
    filterCondition.map { filter => filters += s" $filter"}

    query.append(filters.mkString(" AND "))

    val batchClause = s"ORDER BY $timestampColumn OFFSET $offset ROWS FETCH NEXT $batchSize ROWS ONLY"

    s"$query $batchClause"
  }
}