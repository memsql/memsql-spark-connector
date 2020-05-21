package com.memsql.spark

import java.sql.{Date, Timestamp}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types._

import scala.util.{Random, Try}

class CustomDatatypesTest extends IntegrationSuiteBase {

  val dbName = "testdb"

  def writeRead(dfToWrite: DataFrame,
                expectedDf: DataFrame,
                options: Map[String, String],
                tableName: String): Unit = {
    dfToWrite.write
      .format("memsql")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(s"testdb.$tableName")

    val actualDf =
      spark.read.format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT).load(s"testdb.$tableName")

    assertApproximateDataFrameEquality(
      actualDf,
      expectedDf,
      precision = 0.01,
      orderedComparison = false
    )
  }

  describe("BooleanType") {
    // BooleanType is saved to MemSQL as TINYINT
    // TINYINT is loaded from MemSQL as ShortType
    def testBooleanType(options: Map[String, String], tableName: String): Unit = {
      writeRead(
        spark.createDF(
          List(true, true, false, null).zipWithIndex,
          List(("data", BooleanType, true), ("id", IntegerType, true))
        ),
        spark.createDF(
          List(1: Short, 1: Short, 0: Short, null).zipWithIndex,
          List(("data", ShortType, true), ("id", IntegerType, true))
        ),
        options,
        tableName
      )
    }

    it("LoadDataWriter") {
      testBooleanType(
        Map.empty,
        "BooleanTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testBooleanType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "BooleanTypeInsert"
      )
    }
  }

  describe("ByteType") {
    // ByteType is saved to MemSQL as TINYINT
    // TINYINT is loaded from MemSQL as ShortType
    def testByteType(options: Map[String, String], tableName: String): Unit = {
      writeRead(
        spark.createDF(
          List(Byte.MinValue, 0: Byte, 6: Byte, Byte.MaxValue, null).zipWithIndex,
          List(("data", ByteType, true), ("id", IntegerType, true))
        ),
        spark.createDF(
          List(-128: Short, 0: Short, 6: Short, 127: Short, null).zipWithIndex,
          List(("data", ShortType, true), ("id", IntegerType, true))
        ),
        options,
        tableName
      )
    }

    it("LoadDataWriter") {
      testByteType(
        Map.empty,
        "ByteTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testByteType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "ByteTypeInsert"
      )
    }
  }

  describe("ShortType") {
    // ShortType is saved to MemSQL as SMALLINT
    // SMALLINT is loaded from MemSQL as ShortType
    def testShortType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(Short.MinValue, Short.MaxValue, 0: Short, 5: Short, -100: Short, null).zipWithIndex,
        List(("data", ShortType, true), ("id", IntegerType, true))
      )
      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testShortType(
        Map.empty,
        "ShortTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testShortType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "ShortTypeInsert"
      )
    }
  }

  describe("IntegerType") {
    // IntegerType is saved to MemSQL as INTEGER
    // INTEGER is loaded from MemSQL as IntegerType
    def testIntegerType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 5, -100, null).zipWithIndex,
        List(("data", IntegerType, true), ("id", IntegerType, true))
      )
      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testIntegerType(
        Map.empty,
        "IntegerTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testIntegerType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "IntegerTypeInsert"
      )
    }
  }

  describe("LongType") {
    // LongType is saved to MemSQL as BIGINT
    // BIGINT is loaded from MemSQL as LongType
    def testLongType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(Long.MinValue, Long.MaxValue, 0: Long, 5: Long, -100: Long, null).zipWithIndex,
        List(("data", LongType, true), ("id", IntegerType, true))
      )
      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testLongType(
        Map.empty,
        "LongTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testLongType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "LongTypeInsert"
      )
    }
  }

  describe("FloatType") {
    // FloatType is saved to MemSQL as FLOAT
    // FLOAT is loaded from MemSQL as DoubleType
    def testFloatType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(Float.MinPositiveValue, 0: Float, 5.45.toFloat, -100: Float, null).zipWithIndex,
        List(("data", FloatType, true), ("id", IntegerType, true))
      )
      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testFloatType(
        Map.empty,
        "FloatTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testFloatType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "FloatTypeInsert"
      )
    }
  }

  describe("DoubleType") {
    // DoubleType is saved to MemSQL as DOUBLE
    // DOUBLE is loaded from MemSQL as DoubleType
    def testDoubleType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(Double.MaxValue,
             Double.MinValue,
             Double.MinPositiveValue,
             0.0: Double,
             5.45: Double,
             -100.0: Double,
             null).zipWithIndex,
        List(("data", DoubleType, true), ("id", IntegerType, true))
      )

      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testDoubleType(
        Map.empty,
        "DoubleTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testDoubleType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "DoubleTypeInsert"
      )
    }
  }

  describe("StringType") {
    // StringType is saved to MemSQL as TEXT
    // TEXT is loaded from MemSQL as StringType
    def testStringType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List("",
             "AAAAAAAAAaaaaaa1234567890aaaAaaaaa",
             "strstring",
             null,
             "\\\t\\..<>\n\t\\,@!#$%^&*(\"'").zipWithIndex,
        List(("data", StringType, true), ("id", IntegerType, true))
      )

      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testStringType(
        Map.empty,
        "StringTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testStringType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "StringTypeInsert"
      )
    }
  }

  describe("BinaryType") {
    // BinaryType is saved to MemSQL as BLOB
    // BLOB is loaded from MemSQL as BinaryType
    def testBinaryType(rows: List[Array[Byte]]): Unit = {
      val df = spark.createDF(
        rows.zipWithIndex,
        List(("data", BinaryType, true), ("id", IntegerType, true))
      )

      writeRead(df, df, Map.empty, "BinaryTypeLoad")
      writeRead(df,
                df,
                Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
                "BinaryTypeInsert")
    }

    it("no rows") {
      testBinaryType(List())
    }

    it("one null") {
      testBinaryType(List(null))
    }

    it("a lot of nulls") {
      testBinaryType(List(null, null, null, null, null))
    }

    it("empty row") {
      testBinaryType(List(Array()))
    }

    it("a lot of empty rows") {
      testBinaryType(List(Array(), Array(), Array(), Array()))
    }

    it("one zero byte") {
      testBinaryType(List(Array[Byte](0)))
    }

    it("negative bytes") {
      testBinaryType(List(Array[Byte](-50, -128, -1, -20)))
    }

    it("special characters") {
      testBinaryType(List(Array[Byte]('\t', '\n', '\\')))
    }

    it("row with all possible bytes") {
      testBinaryType(List(Array.range(-128, 127).map(_.toByte)))
    }

    it("a lot of special characters") {
      val specialBytes = Array[Byte](0, 127, -128, '\'', '"', '`', '\\', '/', '\t', '\n', 't', 'n',
        '\f', 'f', '[', ']', '(', ')', '@', '#', ',', '.')
      val sbLen = specialBytes.length

      def genRandomSpecialByte(): Byte = specialBytes(Random.nextInt(sbLen))

      def genRandomRow(): Array[Byte] =
        Array.fill(100)(genRandomSpecialByte())

      testBinaryType(List.fill(100)(genRandomRow()))
    }

    it("big random table") {
      def genRandomByte(): Byte = (Random.nextInt(256) - 128).toByte

      def genRandomRow(): Array[Byte] =
        Array.fill(1000)(genRandomByte())

      testBinaryType(List.fill(1000)(genRandomRow()))
    }
  }

  describe("DateType") {
    // DateType is saved to MemSQL as DATE
    // DATE is loaded from MemSQL as DateType
    def testDateType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(
          Date.valueOf("0001-01-01"),
          Date.valueOf("9999-12-31"),
          Date.valueOf("2001-04-11"),
          Date.valueOf("2001-4-11"),
          Date.valueOf("2020-01-5"),
          null
        ).zipWithIndex,
        List(("data", DateType, true), ("id", IntegerType, true))
      )

      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testDateType(
        Map.empty,
        "DateTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testDateType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "DateTypeInsert"
      )
    }
  }

  describe("TimestampType") {
    // TimestampType is saved to MemSQL as TIMESTAMP(6)
    // TIMESTAMP(6) is loaded from MemSQL as TimestampType
    def testTimestampType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(
          //new Timestamp(999), // Doesn't support [1970-01-01 00:00:00.999]
          //new Timestamp(-10000), // Doesn't support [1969-12-31 23:59:50.0]
          new Timestamp(1000), // Min supported timestamp [1970-01-01 00:00:01.000]
          new Timestamp(12345),
          new Timestamp(21474836L),
          new Timestamp(2147483649L),
          new Timestamp(214748364900L),
          new Timestamp(2147483647999L) // Max supported timestamp [2038-01-19 03:14:07.999]
          //new Timestamp(2147483648000L) // Doesn't support [2038-01-19 03:14:08.000]
          // null // in MemSQL it will be saved as current timestamp
        ).zipWithIndex,
        List(("data", TimestampType, true), ("id", IntegerType, true))
      )

      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testTimestampType(
        Map.empty,
        "TimestampTypeLoad"
      )
    }
    it("BatchInsertWriter") {
      testTimestampType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "TimestampTypeInsert"
      )
    }
  }

  describe("DecimalType") {
    // DecimalType is saved to MemSQL as DECIMAL
    // DECIMAL is loaded from MemSQL as DecimalType
    // in MemSQL DECIMAL max precision is 60 when in spark it is 38
    // during the reading from table with bigger precision values will be truncated
    // in MemSQL DECIMAL max scale is 30 when in spark it is 38
    // during the writing of dataFrame with bigger scale values will be truncated
    def testDecimalType(options: Map[String, String], tableName: String): Unit = {
      val df = spark.createDF(
        List(
          Decimal(123),
          Decimal(123.123),
          Decimal(-123.123.toFloat),
          null
        ).zipWithIndex,
        List(("data", DecimalType(38, 30), true), ("id", IntegerType, true))
      )
      writeRead(df, df, options, tableName)
    }

    it("LoadDataWriter") {
      testDecimalType(
        Map.empty,
        "DecimalTypeLoad"
      )
    }

    it("BatchInsertWriter") {
      testDecimalType(
        Map("tableKey.primary" -> "id", "onDuplicateKeySQL" -> "id = id"),
        "DecimalTypeInsert"
      )
    }

    it("big scale") {
      val df = spark.createDF(
        List(
          Decimal(123),
          Decimal(123.123),
          Decimal(-123.123.toFloat),
          null
        ).zipWithIndex,
        List(("data", DecimalType(38, 32), true), ("id", IntegerType, true))
      )

      val writeResult = Try {
        df.write
          .format("memsql")
          .mode(SaveMode.Overwrite)
          .save(s"testdb.DecimalTypeBigScale")
      }
      assert(writeResult.isFailure)
      assert(
        writeResult.failed.get.getMessage
          .equals("Too big scale specified(32). MemSQL DECIMAL maximum scale is 30"))
    }

    it("big precision") {
      executeQuery("drop table if exists testdb.DecimalTypeBigPrecision")
      executeQuery("create table testdb.DecimalTypeBigPrecision(a DECIMAL(65, 30))")

      val readResult = Try {
        spark.read
          .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
          .load(s"testdb.DecimalTypeBigPrecision")
      }
      assert(readResult.isFailure)
      assert(
        readResult.failed.get.getMessage
          .equals("DECIMAL precision 65 exceeds max precision 38"))
    }
  }

  it("JSON columns are treated as strings by Spark") {
    executeQuery(s"""
                    |create table if not exists ${dbName}.basic (
                    | j JSON
                    |)""".stripMargin)

    spark
      .createDF(
        List("""{"foo":"bar"}"""),
        List(("j", StringType, true))
      )
      .write
      .format("memsql")
      .mode(SaveMode.Append)
      .save("basic")
    val df = spark.read.format("memsql").load("basic")
    assertSmallDataFrameEquality(df,
                                 spark
                                   .createDF(
                                     List("""{"foo":"bar"}"""),
                                     List(("j", StringType, true))
                                   ))
  }

  it("BIT columns are treated as BinaryType") {
    executeQuery("drop table if exists testdb.BIT")
    executeQuery("create table testdb.BIT(a BIT)")
    executeQuery("insert into testdb.BIT values('010101'), ('00'), ('1'), (null)")

    val df = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .load(s"testdb.BIT")
    assertSmallDataFrameEquality(
      df,
      spark.createDF(
        List(
          null,
          Array[Byte](0, 0, '0', '1', '0', '1', '0', '1'),
          Array[Byte](0, 0, 0, 0, 0, 0, '0', '0'),
          Array[Byte](0, 0, 0, 0, 0, 0, 0, '1')
        ),
        List(("a", BinaryType, true))
      ),
      orderedComparison = false
    )
  }

  it("TIME columns are treated as TimestampType") {
    executeQuery("drop table if exists testdb.TIME")
    executeQuery("create table testdb.TIME(a TIME)")
    executeQuery("insert into testdb.TIME values('-838:59:59'), (null), ('838:59:59')")

    val df = spark.read
      .format(DefaultSource.MEMSQL_SOURCE_NAME_SHORT)
      .load(s"testdb.TIME")

    assertSmallDataFrameEquality(
      df,
      spark.createDF(
        List(
          null,
          Timestamp.valueOf("1970-02-04 22:59:59.0"),
          Timestamp.valueOf("1969-11-27 02:59:59")
        ),
        List(("a", TimestampType, true))
      ),
      orderedComparison = false
    )
  }

  // Not supported types:
  // CalendarIntervalType
  // NullType
  // ArrayType
  // MapType

  // Notes
  // BatchInsertWriter fails to write a StringType with null byte ('\0')
  // DECIMAL and DecimalType have different maximum scale and precision. The error will happen if you try to read/write a table/dataFrame with wrong precision/scale
  // TIMESTAMP in MemSQL support values from 1000 to 2147483647999. MemSQL treat null in TIMESTAMP column as current time
}
