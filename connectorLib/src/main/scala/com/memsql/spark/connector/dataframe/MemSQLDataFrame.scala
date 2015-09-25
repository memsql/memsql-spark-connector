package com.memsql.spark.connector.dataframe

import java.sql.{Connection, ResultSet, ResultSetMetaData, Statement}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import com.memsql.spark.connector.rdd.MemSQLRDD
import org.apache.commons.lang.StringEscapeUtils

object MemSQLDataFrameUtils {
  def DataFrameTypeToMemSQLTypeString(dataType: DataType): String = {
    // we match types having _.typeName not a MemSQL type (for instance ShortType.typeName  is "SHORT", but MemSQL calls it "TINYINT")
    dataType match {
      case ShortType => "TINYINT"
      case LongType => "BIGINT"
      case ByteType => "TINYINT"
      case BooleanType => "BOOLEAN"
      case StringType => "TEXT"
      case BinaryType => "BLOB"
      case DecimalType.Unlimited => "DOUBLE"
      case _ => dataType.typeName
    }
  }


  def JDBCTypeToDataFrameType(rsmd: ResultSetMetaData, ix: Int): DataType = {
    rsmd.getColumnType(ix) match {
      case java.sql.Types.CHAR => ByteType
      case java.sql.Types.INTEGER => IntegerType
      case java.sql.Types.TINYINT => ShortType
      case java.sql.Types.SMALLINT => ShortType
      case java.sql.Types.BIGINT => LongType  // TODO: This will prevent inequalities for some dumb reason
      case java.sql.Types.DOUBLE => DoubleType
      case java.sql.Types.NUMERIC => DoubleType
      case java.sql.Types.REAL => FloatType
      case java.sql.Types.BIT => BooleanType
      case java.sql.Types.CLOB => StringType
      case java.sql.Types.BLOB => BinaryType
      case java.sql.Types.TIMESTAMP => TimestampType
      case java.sql.Types.DATE => DateType
      case java.sql.Types.TIME => TimestampType  //srsly?
      case java.sql.Types.DECIMAL => DecimalType(rsmd.getPrecision(ix), rsmd.getScale(ix))
      case java.sql.Types.LONGNVARCHAR => StringType
      case java.sql.Types.LONGVARCHAR => StringType
      case java.sql.Types.VARCHAR => StringType
      case java.sql.Types.NVARCHAR => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.VARBINARY => BinaryType
      case java.sql.Types.BINARY => BinaryType
      case _ => throw new IllegalArgumentException("Can't translate type " + rsmd.getColumnTypeName(ix))
    }
  }

  def GetJDBCValue(dataType: Int, ix: Int, row: ResultSet): Any = {
    val result = dataType match {
      case java.sql.Types.CHAR => row.getString(ix)
      case java.sql.Types.INTEGER => row.getInt(ix)
      case java.sql.Types.BIGINT => row.getLong(ix)
      case java.sql.Types.TINYINT => row.getShort(ix)
      case java.sql.Types.SMALLINT => row.getShort(ix)
      case java.sql.Types.DOUBLE => row.getDouble(ix)
      case java.sql.Types.NUMERIC => row.getDouble(ix)
      case java.sql.Types.REAL => row.getDouble(ix)
      case java.sql.Types.BIT => row.getBoolean(ix)
      case java.sql.Types.CLOB => row.getString(ix)
      case java.sql.Types.BLOB => row.getClob(ix)
      case java.sql.Types.TIMESTAMP => row.getString(ix)
      case java.sql.Types.DATE => row.getDate(ix)
      case java.sql.Types.TIME => row.getTime(ix)
      case java.sql.Types.DECIMAL => row.getBigDecimal(ix)
      case java.sql.Types.LONGNVARCHAR => row.getString(ix)
      case java.sql.Types.LONGVARCHAR => row.getString(ix)
      case java.sql.Types.VARCHAR => row.getString(ix)
      case java.sql.Types.NVARCHAR => row.getString(ix)
      case java.sql.Types.LONGVARBINARY => row.getString(ix)
      case java.sql.Types.VARBINARY => row.getString(ix)
      case java.sql.Types.BINARY => row.getString(ix)
      case _ => throw new IllegalArgumentException("Can't translate type " + dataType.toString)
    }
    if (row.wasNull) null else result
  }
}

object MemSQLDataFrame {
  def MakeMemSQLRowRDD(
    sc: SparkContext,
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    query: String): MemSQLRDD[Row] = {
    new MemSQLRDD(sc, dbHost, dbPort, user, password, dbName, query, (r:ResultSet) => {
      val count = r.getMetaData.getColumnCount
      Row.fromSeq(Range(0, count)
         .map(i => MemSQLDataFrameUtils.GetJDBCValue(r.getMetaData.getColumnType(i + 1), i + 1, r)))
    })
  }

  def MakeMemSQLDF(
    sqlContext: SQLContext,
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    query: String): DataFrame = {
    sqlContext.load("com.memsql.spark.connector.dataframe.MemSQLRelationProvider", Map(
      "dbHost" -> dbHost,
      "dbPort" -> dbPort.toString,
      "user" -> user,
      "password" -> password,
      "dbName" -> dbName,
      "query" -> query))
  }

  def getQuerySchema(
    dbHost: String,
    dbPort: Int,
    user: String,
    password: String,
    dbName: String,
    query: String): StructType = {

    var conn: Connection = null
    var schemaStmt: Statement = null
    try {
      conn = MemSQLRDD.getConnection(dbHost, dbPort, user, password, dbName)
      schemaStmt = conn.createStatement
      val metadata = schemaStmt.executeQuery(limitZero(query)).getMetaData
      val count = metadata.getColumnCount
      StructType(Range(0,count).map(i => StructField(metadata.getColumnName(i + 1),
                                                     MemSQLDataFrameUtils.JDBCTypeToDataFrameType(metadata, i + 1), true)))
    } finally {
      if (schemaStmt != null && !schemaStmt.isClosed()) {
        schemaStmt.close()
      }
      if (null != conn && !conn.isClosed()) {
        conn.close()
      }
    }
  }

  def limitZero(q: String): String = "SELECT * FROM (" + q + ") tab_alias LIMIT 0"
}

case class MemSQLScan(@transient val rdd: MemSQLRDD[Row], @transient val sqlContext: SQLContext)
   extends BaseRelation with PrunedFilteredScan {
  val schema : StructType = MemSQLDataFrame.getQuerySchema(rdd.dbHost, rdd.dbPort, rdd.user, rdd.password, rdd.dbName, rdd.sql)

  private def getWhere(filters: Array[Filter]): String = {
    val result = new StringBuilder
    for (i <- 0 to (filters.size - 1)) {
      if (i != 0) { // scala apparently has no "pythonic" join
        result.append(" AND ")
      }
      filters(i) match {
        case EqualTo(attr, v) =>  result.append(attr).append(" = '").append(StringEscapeUtils.escapeSql(v.toString)).append("'")
        case GreaterThan(attr, v) =>  result.append(attr).append(" > '").append(StringEscapeUtils.escapeSql(v.toString)).append("'")
        case LessThan(attr, v) =>  result.append(attr).append(" < '").append(StringEscapeUtils.escapeSql(v.toString)).append("'")
        case GreaterThanOrEqual(attr, v) => result.append(attr).append(" >= '").append(StringEscapeUtils.escapeSql(v.toString)).append("'")
        case LessThanOrEqual(attr, v) => result.append(attr).append(" <= '").append(StringEscapeUtils.escapeSql(v.toString)).append("'")
        case In(attr, vs) => {
          result.append(" in (")
          for (j <- 0 to (vs.size - 1)) {
            if (j != 0) {
              result.append(",")
            }
            result.append("'").append(StringEscapeUtils.escapeSql(vs(j).toString)).append("'")
          }
          result.append(" )")
        }
      }
    }
    result.toString
  }
  private def getProject(requiredColumns: Array[String]): String = {
    if (requiredColumns.size == 0) { // for df.count, df.is_empty
      "1"
    } else {
      requiredColumns.mkString(",")
    }
  }

  def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    var sql = "SELECT " + getProject(requiredColumns) + " FROM (" + rdd.sql + ") tab_alias"
    if (filters.size != 0) {
      sql = sql + " WHERE " + getWhere(filters)
    }
    MemSQLDataFrame.MakeMemSQLRowRDD(sqlContext.sparkContext, rdd.dbHost, rdd.dbPort, rdd.user, rdd.password, rdd.dbName, sql)
  }
}

class MemSQLRelationProvider extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String,String]): MemSQLScan = {
    new MemSQLScan(MemSQLDataFrame.MakeMemSQLRowRDD(sqlContext.sparkContext, parameters("dbHost"),
                          parameters("dbPort").toInt, parameters("user"), parameters("password"),
                          parameters("dbName"), parameters("query")), sqlContext)
  }
}
