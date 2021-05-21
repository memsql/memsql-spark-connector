package com.singlestore.spark

import java.sql.Connection

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}

object SinglestoreConnectionFactory {

  var connectionPool: Map[JDBCOptions, () => Connection] = Map.empty[JDBCOptions, () => Connection]

  def createConnectionFactory(options: JDBCOptions): () => Connection = {
    val connectionFactory = JdbcUtils.createConnectionFactory(options)
    connectionPool += (options -> connectionFactory)
    connectionFactory
  }

  def getConnection(options: JDBCOptions): Connection = {
    connectionPool.get(options) match {
      case Some(connectionFactory) => connectionFactory()
      case None =>
        val connectionFactory = createConnectionFactory(options)
        connectionFactory()
    }
  }
}
