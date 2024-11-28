package org.sunbird.obsrv.connector

import org.sunbird.obsrv.connector.model.Models.ConnectorContext

class PostgresSource extends IJDBCSource {

  override def getDriver(): String = "org.postgresql.Driver"

}