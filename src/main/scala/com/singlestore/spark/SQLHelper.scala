package com.singlestore.spark

import com.singlestore.spark.JdbcHelpers.{executeQuery, getDDLJDBCOptions}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.{Row, SparkSession}

object SQLHelper extends LazyLogging {
  implicit class QueryMethods(spark: SparkSession) {
    private def singlestoreQuery(db: Option[String],
                                 query: String,
                                 variables: Any*): Iterator[Row] = {
      val ctx = spark.sqlContext
      var opts = ctx.getAllConfs.collect {
        case (k, v) if k.startsWith(DefaultSource.SINGLESTORE_GLOBAL_OPTION_PREFIX) =>
          k.stripPrefix(DefaultSource.SINGLESTORE_GLOBAL_OPTION_PREFIX) -> v
        case (k, v) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
          k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v
      }

      if (db.isDefined) {
        val dbValue = db.get
        if (dbValue.isEmpty) {
          opts -= "database"
        } else {
          opts += ("database" -> dbValue)
        }
      }

      val conf = SinglestoreOptions(CaseInsensitiveMap(opts))
      val conn = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
      try {
        executeQuery(conn, query, variables: _*)
      } finally {
        conn.close()
      }
    }

    def executeSinglestoreQueryDB(db: String, query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(Some(db), query, variables: _*)
    }

    def executeSinglestoreQuery(query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(None, query, variables: _*)
    }

    @Deprecated
    def executeMemsqlQueryDB(db: String, query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(Some(db), query, variables: _*)
    }

    @Deprecated
    def executeMemsqlQuery(query: String, variables: Any*): Iterator[Row] = {
      singlestoreQuery(None, query, variables: _*)
    }
  }

  def executeSinglestoreQueryDB(spark: SparkSession,
                                db: String,
                                query: String,
                                variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreQueryDB(db, query, variables: _*)
  }

  def executeSinglestoreQuery(spark: SparkSession,
                              query: String,
                              variables: Any*): Iterator[Row] = {
    spark.executeSinglestoreQuery(query, variables: _*)
  }
  @Deprecated
  def executeMemsqlQueryDB(spark: SparkSession,
                           db: String,
                           query: String,
                           variables: Any*): Iterator[Row] = {
    spark.executeMemsqlQueryDB(db, query, variables: _*)
  }

  @Deprecated
  def executeMemsqlQuery(spark: SparkSession, query: String, variables: Any*): Iterator[Row] = {
    spark.executeMemsqlQuery(query, variables: _*)
  }
}
