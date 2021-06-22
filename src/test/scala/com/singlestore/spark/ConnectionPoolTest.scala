package com.singlestore.spark

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.mariadb.jdbc.MariaDbPoolDataSource
import org.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Random

class ConnectionPoolTest
    extends IntegrationSuiteBase
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MockitoSugar {

  val tableName = "connectionpool"

  describe("success tests") {

    it("same records amount") {

      val loopSize = 100
      val dfSize   = 10

      for (_ <- 1 to loopSize) {
        new Thread {
          val df: DataFrame =
            spark.createDF(
              List.fill(dfSize)((Random.nextLong(), Random.nextInt())),
              List(("LongType", LongType, true), ("IntType", IntegerType, true))
            )
          df.write
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .mode(SaveMode.Append)
            .save(s"testdb.$tableName")
        }.start()
      }
      val actualDf = spark.read
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .load(s"testdb.$tableName")

      assert(actualDf.collect().length == loopSize * dfSize)
    }

    it("connection pool size") {

      // First connection created in `beforeAll` method
      val connectionPoolField =
        SinglestoreConnectionFactory.getClass
          .getDeclaredField("connectionPool")
      connectionPoolField.setAccessible(true)

      val connectionPoolBefore = connectionPoolField
        .get(SinglestoreConnectionFactory)
        .asInstanceOf[Map[String, MariaDbPoolDataSource]]

      val beforeSize = connectionPoolBefore.size

      val dfSize = 10

      val df: DataFrame =
        spark.createDF(
          List.fill(dfSize)((Random.nextLong(), Random.nextInt())),
          List(("LongType", LongType, true), ("IntType", IntegerType, true))
        )

      // First connection
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .option("option1", "value1")
        .option("option2", "value2")
        .option("option3", "value3")
        .mode(SaveMode.Append)
        .save(s"testdb.$tableName")

      // First connection
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .option("option2", "value2")
        .option("option1", "value1")
        .option("option3", "value3")
        .mode(SaveMode.Append)
        .save(s"testdb.$tableName")

      // First connection
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .option("option3", "value3")
        .option("option2", "value2")
        .option("option1", "value1")
        .mode(SaveMode.Append)
        .save(s"testdb.$tableName")

      // Second connection
      df.write
        .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
        .option("option2", "value2")
        .option("option1", "value1")
        .mode(SaveMode.Append)
        .save(s"testdb.$tableName")

      val connectionPoolAfter = connectionPoolField
        .get(SinglestoreConnectionFactory)
        .asInstanceOf[Map[String, MariaDbPoolDataSource]]

      val afterSize = connectionPoolAfter.size

      assert(afterSize - beforeSize == 2)
    }
  }

}
