package com.singlestore.spark

import java.sql.{PreparedStatement, SQLException}

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.singlestore.spark.JdbcHelpers.getDDLJDBCOptions
import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class ExternalHostTest
    extends IntegrationSuiteBase
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MockitoSugar {

  val testDb            = "testdb"
  val testCollection    = "externalHost"
  val mvNodesCollection = "mv_nodes"

  var df: DataFrame = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    df = spark.createDF(
      List((2, "B")),
      List(("id", IntegerType, true), ("name", StringType, true))
    )
    writeTable(s"$testDb.$testCollection", df)
  }

  def setupMockJdbcHelper(): Unit = {
    when(JdbcHelpers.loadSchema(any[SinglestoreOptions], any[String], any[SQLGen.VariableList]))
      .thenCallRealMethod()
    when(JdbcHelpers.getDDLJDBCOptions(any[SinglestoreOptions])).thenCallRealMethod()
    when(JdbcHelpers.getDMLJDBCOptions(any[SinglestoreOptions])).thenCallRealMethod()
    when(JdbcHelpers.getJDBCOptions(any[SinglestoreOptions], any[String])).thenCallRealMethod()
    when(JdbcHelpers.explainJSONQuery(any[SinglestoreOptions], any[String], any[VariableList]))
      .thenCallRealMethod()
    when(JdbcHelpers.partitionHostPorts(any[SinglestoreOptions], any[String]))
      .thenCallRealMethod()
    when(JdbcHelpers.fillStatement(any[PreparedStatement], any[VariableList]))
      .thenCallRealMethod()
  }

  describe("success tests") {
    it("low SingleStore version") {

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("6.8.10")

        val actualDF =
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")

        assertSmallDataFrameEquality(
          actualDF,
          df
        )
      }
    }

    it("valid external host") {

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")

        val externalHostMap = Map(
          "172.17.0.2:3307" -> "172.17.0.2:3307"
        )
        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(externalHostMap)

        val actualDF =
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")

        assertSmallDataFrameEquality(
          actualDF,
          df
        )
      }
    }

    it("empty external host map") {

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")
        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(Map.empty[String, String])

        val actualDf = spark.read
          .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
          .option("useExternalHost", "true")
          .load(s"$testDb.$testCollection")

        assertSmallDataFrameEquality(df, actualDf)
      }
    }

    it("wrong external host map") {

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")

        val externalHostMap = Map(
          "172.17.0.3:3307" -> "172.17.0.100:3307",
          "172.17.0.4:3307" -> "172.17.0.200:3307"
        )

        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(externalHostMap)

        val actualDf = spark.read
          .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
          .option("useExternalHost", "true")
          .load(s"$testDb.$testCollection")

        assertSmallDataFrameEquality(df, actualDf)
      }
    }

    it("valid external host function") {

      val mvNodesDf = spark.createDF(
        List(("172.17.0.2", 3307, "172.17.0.10", 3310),
             ("172.17.0.20", 3312, "172.17.0.100", null),
             ("172.17.0.2", 3308, null, 3310),
             ("172.17.0.15", 3311, null, null)),
        List(("IP_ADDR", StringType, true),
             ("PORT", IntegerType, true),
             ("EXTERNAL_HOST", StringType, true),
             ("EXTERNAL_PORT", IntegerType, true))
      )
      writeTable(s"$testDb.$mvNodesCollection", mvNodesDf)

      val conf = new SinglestoreOptions(
        s"$masterHost:$masterPort",
        List.empty[String],
        "root",
        masterPassword,
        None,
        Map.empty[String, String],
        false,
        false,
        true,
        Truncate,
        SinglestoreOptions.CompressionType.GZip,
        SinglestoreOptions.LoadDataFormat.CSV,
        List.empty[SinglestoreOptions.TableKey],
        None,
        10,
        10
      )

      val conn         = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
      val statement    = conn.prepareStatement(s"""
        SELECT IP_ADDR,    
        PORT,
        EXTERNAL_HOST,         
        EXTERNAL_PORT
        FROM testdb.mv_nodes;
      """)
      val spyConn      = spy(conn)
      val spyStatement = spy(statement)
      when(spyConn.prepareStatement(s"""
        SELECT IP_ADDR,    
        PORT,
        EXTERNAL_HOST,         
        EXTERNAL_PORT
        FROM INFORMATION_SCHEMA.MV_NODES 
        WHERE TYPE = "LEAF";
      """)).thenReturn(spyStatement)

      withObjectMocked[JdbcUtils.type] {

        when(JdbcUtils.createConnectionFactory(any[JDBCOptions])).thenReturn(() => spyConn)
        val externalHostPorts = JdbcHelpers.externalHostPorts(conf)
        val expectedResult = Map(
          "172.17.0.2:3307" -> "172.17.0.10:3310"
        )
        assert(externalHostPorts.equals(expectedResult))
      }
    }
  }

  describe("failed tests") {

    it("wrong external host") {

      withObjectMocked[JdbcHelpers.type] {

        setupMockJdbcHelper()
        when(JdbcHelpers.getSinglestoreVersion(any[SinglestoreOptions])).thenReturn("7.1.0")

        val externalHostMap = Map(
          "172.17.0.2:3307" -> "somehost:3307"
        )
        when(JdbcHelpers.externalHostPorts(any[SinglestoreOptions]))
          .thenReturn(externalHostMap)

        try {
          spark.read
            .format(DefaultSource.SINGLESTORE_SOURCE_NAME_SHORT)
            .option("useExternalHost", "true")
            .load(s"$testDb.$testCollection")
            .collect()
          fail("Exception expected")
        } catch {
          case ex: Throwable =>
            ex.getCause match {
              case sqlEx: SQLException =>
                assert(
                  sqlEx.getMessage startsWith "No active connection found for master : Could not connect to HostAddress{host='somehost', port=3307, type='master'}. ")
              case _ => fail("SQLException expected")
            }
        }
      }
    }
  }
}
