package com.singlestore.spark

import java.sql.{Connection, PreparedStatement, Statement, SQLException}

import com.singlestore.spark.SinglestoreOptions.{TableKey, TableKeyType}
import com.singlestore.spark.SQLGen.{StringVar, VariableList}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{StringType, StructType}

import scala.util.Try

case class SinglestorePartitionInfo(ordinal: Int, name: String, hostport: String)

object JdbcHelpers extends LazyLogging {
  final val SINGLESTORE_CONNECT_TIMEOUT = "10000" // 10 seconds in ms

  // register the SingleStoreDialect
  JdbcDialects.registerDialect(SinglestoreDialect)

  // Connection implicits
  implicit class ConnectionHelpers(val conn: Connection) {
    def withStatement[T](handle: Statement => T): T =
      Loan(conn.createStatement()).to(handle)

    def withPreparedStatement[T](query: String, handle: PreparedStatement => T): T =
      Loan(conn.prepareStatement(query)).to(handle)
  }

  def getJDBCOptions(conf: SinglestoreOptions, hostPorts: String*): JDBCOptions = {
    val url: String = {
      val base = s"jdbc:mysql:loadbalance://${hostPorts.mkString(",")}"
      conf.database match {
        case Some(d) => s"$base/$d"
        case None    => base
      }
    }

    val sessionVariables = Seq(
      "collation_server=utf8_general_ci",
      "sql_select_limit=18446744073709551615",
      "compile_only=false",
      "sql_mode='STRICT_ALL_TABLES,ONLY_FULL_GROUP_BY'"
    ).mkString(";")

    new JDBCOptions(
      Map(
        JDBCOptions.JDBC_URL          -> url,
        JDBCOptions.JDBC_TABLE_NAME   -> "XXX",
        JDBCOptions.JDBC_DRIVER_CLASS -> "org.mariadb.jdbc.Driver",
        "user"                        -> conf.user,
        "password"                    -> conf.password,
        "zeroDateTimeBehavior"        -> "convertToNull",
        "allowLoadLocalInfile"        -> "true",
        "connectTimeout"              -> SINGLESTORE_CONNECT_TIMEOUT,
        "sessionVariables"            -> sessionVariables
      ) ++ conf.jdbcExtraOptions
    )
  }

  def getDDLJDBCOptions(conf: SinglestoreOptions): JDBCOptions =
    getJDBCOptions(conf, conf.ddlEndpoint)

  def getDMLJDBCOptions(conf: SinglestoreOptions): JDBCOptions =
    getJDBCOptions(conf, conf.dmlEndpoints: _*)

  def executeQuery(conn: Connection, query: String, variables: Any*): Iterator[Row] = {
    val statement = conn.prepareStatement(query)
    try {
      fillStatementJdbc(statement, variables.toList)
      val rs     = statement.executeQuery()
      val schema = JdbcUtils.getSchema(rs, SinglestoreDialect, alwaysNullable = true)
      JdbcUtils.resultSetToRows(rs, schema)
    } finally {
      statement.close()
    }
  }

  def loadSchema(conf: SinglestoreOptions, query: String, variables: VariableList): StructType = {
    val conn = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
    try {
      val statement =
        conn.prepareStatement(SinglestoreDialect.getSchemaQuery(s"($query) AS q"))
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, SinglestoreDialect, alwaysNullable = true)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  def explainQuery(conf: SinglestoreOptions, query: String, variables: VariableList): String = {
    val conn = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
    try {
      val statement = conn.prepareStatement(s"EXPLAIN $query")
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          var out = List.empty[String]
          while (rs.next) {
            out = rs.getString(1) :: out
          }
          out.reverseIterator.mkString("\n")
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  // explainJSONQuery runs `EXPLAIN JSON` on the query and returns the String
  // representing this queries plan as JSON.
  def explainJSONQuery(conf: SinglestoreOptions, query: String, variables: VariableList): String = {
    val conn = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
    try {
      val statement = conn.prepareStatement(s"EXPLAIN JSON ${query}")
      try {
        fillStatement(statement, variables)
        val rs = statement.executeQuery()
        try {
          // we only expect one row in the output
          if (!rs.next()) { assert(false, "EXPLAIN JSON failed to return a row") }
          val json = rs.getString(1)
          assert(!rs.next(), "EXPLAIN JSON returned more than one row")
          json
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  // partitionHostPorts returns a list of (ordinal, name, host:port) for all master
  // partitions in the specified database
  def partitionHostPorts(conf: SinglestoreOptions,
                         database: String): List[SinglestorePartitionInfo] = {
    val conn = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
    try {
      val statement = conn.prepareStatement(s"""
        SELECT HOST, PORT
        FROM INFORMATION_SCHEMA.DISTRIBUTED_PARTITIONS
        WHERE DATABASE_NAME = ? AND ROLE = "Master"
        ORDER BY ORDINAL ASC
      """)
      try {
        fillStatement(statement, List(StringVar(database)))
        val rs = statement.executeQuery()
        try {
          var out = List.empty[SinglestorePartitionInfo]
          var idx = 0
          while (rs.next) {
            out = SinglestorePartitionInfo(idx,
                                           s"${database}_${idx}",
                                           s"${rs.getString(1)}:${rs.getInt(2)}") :: out
            idx += 1
          }
          out.reverse
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
    * Return map from original host/port to external host/port
    * @param conf options
    * @return Map `host:port` -> `externalHost:externalPort`
    */
  def externalHostPorts(conf: SinglestoreOptions): Map[String, String] = {
    val conn = SinglestoreConnectionFactory.getConnection(getDDLJDBCOptions(conf))
    try {
      val statement = conn.prepareStatement(s"""
        SELECT IP_ADDR,    
        PORT,
        EXTERNAL_HOST,         
        EXTERNAL_PORT
        FROM INFORMATION_SCHEMA.MV_NODES 
        WHERE TYPE = "LEAF";
      """)
      try {
        val rs = statement.executeQuery()
        try {
          var out = Map.empty[String, String]
          while (rs.next) {
            val host               = rs.getString(1)
            val port               = rs.getInt(2)
            val externalHost       = rs.getString(3)
            val externalPortString = rs.getString(4)
            if (externalHost != null && externalPortString != null) {
              val externalPort = externalPortString.toInt
              out = out + (s"$host:$port" -> s"$externalHost:$externalPort")
            }
          }
          out
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  def fillStatement(stmt: PreparedStatement, variables: VariableList): Unit = {
    import SQLGen._
    if (variables.isEmpty) { return }

    variables.zipWithIndex.foreach {
      case (StringVar(v), index) => stmt.setString(index + 1, v)
      case (IntVar(v), index)    => stmt.setInt(index + 1, v)
      case (LongVar(v), index)   => stmt.setLong(index + 1, v)
      case (ShortVar(v), index)  => stmt.setShort(index + 1, v)
      case (FloatVar(v), index)  => stmt.setFloat(index + 1, v)
      case (DoubleVar(v), index) => stmt.setDouble(index + 1, v)
      case (DecimalVar(v), index) =>
        stmt.setBigDecimal(index + 1, v.toJavaBigDecimal)
      case (BooleanVar(v), index)   => stmt.setBoolean(index + 1, v)
      case (ByteVar(v), index)      => stmt.setByte(index + 1, v)
      case (DateVar(v), index)      => stmt.setDate(index + 1, v)
      case (TimestampVar(v), index) => stmt.setTimestamp(index + 1, v)
      case (v, _) =>
        throw new IllegalArgumentException(
          "Unexpected Variable Type: " + v.getClass.getName
        )
    }
  }

  def fillStatementJdbc(stmt: PreparedStatement, variables: List[Any]): Unit = {
    // here we leave it to JDBC driver to do type conversions
    if (variables.isEmpty) { return }
    for ((v, index) <- variables.zipWithIndex) {
      stmt.setObject(index + 1, v)
    }
  }

  def schemaToString(schema: StructType, tableKeys: List[TableKey]): String = {
    // spark should never call any of our code if the schema is empty
    assert(schema.length > 0)

    val fieldsSql = schema.fields
      .map(field => {
        val name = SinglestoreDialect.quoteIdentifier(field.name)
        val typ = SinglestoreDialect
          .getJDBCType(field.dataType)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Can't get JDBC type for ${field.dataType.simpleString}"
            )
          )
        val nullable  = if (field.nullable) "" else " NOT NULL"
        val collation = if (field.dataType == StringType) " COLLATE UTF8_BIN" else ""
        s"${name} ${typ.databaseTypeDefinition}${collation}${nullable}"
      })

    // we want to default all tables to columnstore, but in 6.8 and below you *must*
    // specify a sort key so we just pick the first column arbitrarily for now
    var finalTableKeys = tableKeys
    // if all the keys are shard keys it means there are no other keys so we can default
    if (tableKeys.forall(_.keyType == TableKeyType.Shard)) {
      finalTableKeys = TableKey(TableKeyType.Columnstore, columns = schema.head.name) :: tableKeys
    }

    def keyNameColumnsSQL(key: TableKey) =
      s"${key.name.map(SinglestoreDialect.quoteIdentifier).getOrElse("")}(${key.columns})"

    val keysSql = finalTableKeys.map {
      case key @ TableKey(TableKeyType.Primary, _, _) => s"PRIMARY KEY ${keyNameColumnsSQL(key)}"
      case key @ TableKey(TableKeyType.Columnstore, _, _) =>
        s"KEY ${keyNameColumnsSQL(key)} USING CLUSTERED COLUMNSTORE"
      case key @ TableKey(TableKeyType.Unique, _, _) => s"UNIQUE KEY ${keyNameColumnsSQL(key)}"
      case key @ TableKey(TableKeyType.Shard, _, _)  => s"SHARD KEY ${keyNameColumnsSQL(key)}"
      case key @ TableKey(TableKeyType.Key, _, _)    => s"KEY ${keyNameColumnsSQL(key)}"
    }

    (fieldsSql ++ keysSql).mkString("(\n  ", ",\n  ", "\n)")
  }

  def tableExists(conn: Connection, table: TableIdentifier): Boolean = {
    conn.withStatement(
      stmt =>
        Try {
          try {
            stmt.execute(SinglestoreDialect.getTableExistsQuery(table.quotedString))
          } finally {
            stmt.close()
          }
        }.isSuccess
    )
  }

  def getSinglestoreVersion(conf: SinglestoreOptions): String = {
    val jdbcOpts = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn     = SinglestoreConnectionFactory.getConnection(jdbcOpts)
    val sql      = "select @@memsql_version"
    log.trace(s"Executing SQL:\n$sql")
    val resultSet = conn.withStatement(stmt => {
      try {
        stmt.executeQuery(sql)
      } catch {
        case _: SQLException => throw new IllegalArgumentException("Can't get SingleStore version")
      } finally {
        stmt.close()
        conn.close()
      }
    })
    if (resultSet.next()) {
      resultSet.getString("@@memsql_version")
    } else throw new IllegalArgumentException("Can't get SingleStore version")
  }

  def createTable(conn: Connection,
                  table: TableIdentifier,
                  schema: StructType,
                  tableKeys: List[TableKey]): Unit = {
    val sql = s"CREATE TABLE ${table.quotedString} ${schemaToString(schema, tableKeys)}"
    log.trace(s"Executing SQL:\n$sql")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def truncateTable(conn: Connection, table: TableIdentifier): Unit = {
    val sql = s"TRUNCATE TABLE ${table.quotedString}"
    log.trace(s"Executing SQL:\n$sql")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def dropTable(conn: Connection, table: TableIdentifier): Unit = {
    val sql = s"DROP TABLE ${table.quotedString}"
    log.trace(s"Executing SQL:\n$sql")
    conn.withStatement(stmt => stmt.executeUpdate(sql))
  }

  def isReferenceTable(conf: SinglestoreOptions, table: TableIdentifier): Boolean = {
    val jdbcOpts = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn     = SinglestoreConnectionFactory.getConnection(jdbcOpts)
    // Assume that either table.database is set or conf.database is set
    val databaseName =
      table.database
        .orElse(conf.database)
        .getOrElse(throw new IllegalArgumentException("Database name should be defined"))
    val sql = s"using $databaseName show tables extended like '${table.table}'"
    log.trace(s"Executing SQL:\n$sql")
    val resultSet = conn.withStatement(stmt => {
      Try {
        try {
          stmt.executeQuery(sql)
        } finally {
          stmt.close()
          conn.close()
        }
      }
    })
    resultSet.toOption.fold(false)(resultSet => {
      if (resultSet.next()) {
        !resultSet.getBoolean("distributed")
      } else {
        throw new IllegalArgumentException(s"Table `$databaseName.${table.table}` doesn't exist")
      }
    })
  }

  def prepareTableForWrite(conf: SinglestoreOptions,
                           table: TableIdentifier,
                           mode: SaveMode,
                           schema: StructType): Unit = {
    val jdbcOpts = JdbcHelpers.getDDLJDBCOptions(conf)
    val conn     = SinglestoreConnectionFactory.getConnection(jdbcOpts)
    try {
      if (JdbcHelpers.tableExists(conn, table)) {
        mode match {
          case SaveMode.Overwrite =>
            conf.overwriteBehavior match {
              case Truncate =>
                JdbcHelpers.truncateTable(conn, table)
              case DropAndCreate =>
                JdbcHelpers.dropTable(conn, table)
                JdbcHelpers.createTable(conn, table, schema, conf.tableKeys)
              case Merge =>
              // nothing to do
            }
          case SaveMode.ErrorIfExists =>
            sys.error(
              s"Table '${table}' already exists. SaveMode: ErrorIfExists."
            )
          case SaveMode.Ignore =>
          // table already exists, nothing to do
          case SaveMode.Append => // continue
        }
      } else {
        JdbcHelpers.createTable(conn, table, schema, conf.tableKeys)
      }
    } finally {
      conn.close()
    }
  }
}
