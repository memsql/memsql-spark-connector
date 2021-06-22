package com.singlestore.spark

import java.sql.Connection

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.mariadb.jdbc.MariaDbPoolDataSource

import scala.collection.JavaConversions._

object SinglestoreConnectionFactory {

  private var connectionPool: Map[String, MariaDbPoolDataSource] =
    Map.empty[String, MariaDbPoolDataSource]

  private def createDataSource(options: JDBCOptions, key: String): MariaDbPoolDataSource = {
    val url        = options.url
    val properties = options.asProperties

    val stringProperties = properties
      .stringPropertyNames()
      .flatMap({
        // Remove url from properties
        case "url" => None
        // Create pairs <propertyName>=<propertyValue>
        case propertyName => Some(s"$propertyName=${properties.getProperty(propertyName)}")
      })
      .mkString("&")

    val finalUrl = s"$url?$stringProperties"
    val ds       = new MariaDbPoolDataSource(finalUrl)
    ds.setPoolValidMinDelay(0)

    connectionPool += (key -> ds)
    ds
  }

  def getConnection(options: JDBCOptions): Connection = {
    val key = options.asProperties.toString
    val ds = connectionPool.get(key) match {
      case Some(ds) => ds
      case None =>
        createDataSource(options, key)
    }
    ds.getConnection
  }

  def closeConnections(): Unit = {
    connectionPool.values.foreach(ds => ds.close())
  }
}
