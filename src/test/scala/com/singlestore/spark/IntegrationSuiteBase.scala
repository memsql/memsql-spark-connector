package com.singlestore.spark

import java.util.TimeZone

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import com.singlestore.spark.JdbcHelpers.executeQuery
import com.singlestore.spark.SQLHelper._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.scalatest.funspec.AnyFunSpec

import scala.util.Random

trait IntegrationSuiteBase
    extends AnyFunSpec
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with DataFrameComparer
    with LazyLogging {
  final val masterHost: String = sys.props.getOrElse("singlestore.host", "localhost")
  final val masterPort: String = sys.props.getOrElse("singlestore.port", "5506")

  final val continuousIntegration: Boolean = sys.env
    .getOrElse("CONTINUOUS_INTEGRATION", "false") == "true"

  final val masterPassword: String = sys.env.getOrElse("SINGLESTORE_PASSWORD", "")

  var spark: SparkSession = _

  var jdbcOptsDefault = new JDBCOptions(
    Map(
      JDBCOptions.JDBC_URL          -> s"jdbc:mysql://$masterHost:$masterPort",
      JDBCOptions.JDBC_TABLE_NAME   -> "XXX",
      JDBCOptions.JDBC_DRIVER_CLASS -> "org.mariadb.jdbc.Driver",
      "user"                        -> "root",
      "password"                    -> masterPassword
    )
  )

  override def beforeAll(): Unit = {
    // override global JVM timezone to GMT
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))

    val conn = SinglestoreConnectionFactory.getConnection(jdbcOptsDefault)
    try {
      // make SingleStore use less memory
      executeQuery(conn, "set global default_partitions_per_leaf = 2")

      executeQuery(conn, "drop database if exists testdb")
      executeQuery(conn, "create database testdb")
    } finally {
      conn.close()
    }
  }

  override def withFixture(test: NoArgTest): Outcome = {
    def retryThrowable(t: Throwable): Boolean = t match {
      case _: java.sql.SQLNonTransientConnectionException => true
      case _                                              => false
    }

    @scala.annotation.tailrec
    def runWithRetry(attempts: Int, lastError: Option[Throwable]): Outcome = {
      if (attempts == 0) {
        return Canceled(
          s"too many SQLNonTransientConnectionExceptions occurred, last error was:\n${lastError.get}")
      }

      super.withFixture(test) match {
        case Failed(t: Throwable) if retryThrowable(t) || retryThrowable(t.getCause) => {
          Thread.sleep(3000)
          runWithRetry(attempts - 1, Some(t))
        }
        case other => other
      }
    }

    runWithRetry(attempts = 5, None)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    val seed = Random.nextLong()
    log.debug("Random seed: " + seed)
    Random.setSeed(seed)

    if (!continuousIntegration) {
      LogManager.getLogger("com.singlestore.spark").setLevel(Level.TRACE)
    }

    spark = SparkSession
      .builder()
      .master("local")
      .appName("singlestore-integration-tests")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.bindAddress", "localhost")
      .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
      .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
      .config("spark.sql.session.timeZone", "GMT")
      .config("spark.datasource.singlestore.ddlEndpoint", s"${masterHost}:${masterPort}")
      .config("spark.datasource.singlestore.user", "root-ssl")
      .config("spark.datasource.singlestore.password", "")
      .config("spark.datasource.singlestore.enableAsserts", "true")
      .config("spark.datasource.singlestore.enableParallelRead", "true")
      .config("spark.datasource.singlestore.database", "testdb")
      .config("spark.datasource.singlestore.useSSL", "true")
      .config("spark.datasource.singlestore.serverSslCert",
              s"${System.getProperty("user.dir")}/scripts/ssl/test-ca-cert.pem")
      .config("spark.datasource.singlestore.disableSslHostnameVerification", "true")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    spark.close()
  }

  def executeQueryWithLog(sql: String): Unit = {
    log.trace(s"executing query: ${sql}")
    spark.executeSinglestoreQuery(sql)
  }

  def jdbcOptions(dbtable: String): Map[String, String] = Map(
    "url"      -> s"jdbc:mysql://$masterHost:$masterPort",
    "dbtable"  -> dbtable,
    "user"     -> "root",
    "password" -> masterPassword
  )

  def jdbcOptionsSQL(dbtable: String): String =
    jdbcOptions(dbtable)
      .foldLeft(List.empty[String])({
        case (out, (k, v)) => s"'${k}'='${v}'" :: out
      })
      .mkString(", ")

  def writeTable(dbtable: String, df: DataFrame, saveMode: SaveMode = SaveMode.Overwrite): Unit =
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .mode(saveMode)
      .save(dbtable)

  def insertValues(dbtable: String,
                   df: DataFrame,
                   onDuplicateKeySQL: String,
                   insertBatchSize: Long): Unit =
    df.write
      .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
      .option("onDuplicateKeySQL", onDuplicateKeySQL)
      .option("insertBatchSize", insertBatchSize)
      .mode(SaveMode.Append)
      .save(dbtable)
}
