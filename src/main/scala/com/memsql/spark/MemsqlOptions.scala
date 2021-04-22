package com.memsql.spark

import com.memsql.spark.MemsqlOptions.TableKey
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

case class MemsqlOptions(
    ddlEndpoint: String,
    dmlEndpoints: List[String],
    user: String,
    password: String,
    database: Option[String],
    jdbcExtraOptions: Map[String, String],
    enableAsserts: Boolean,
    disablePushdown: Boolean,
    enableParallelRead: Boolean,
    useExternalHost: Boolean,
    // write options
    overwriteBehavior: OverwriteBehavior,
    loadDataCompression: MemsqlOptions.CompressionType.Value,
    loadDataFormat: MemsqlOptions.LoadDataFormat.Value,
    tableKeys: List[TableKey],
    onDuplicateKeySQL: Option[String],
    insertBatchSize: Int
) extends LazyLogging {

  def assert(condition: Boolean, message: String) = {
    if (!condition) {
      if (enableAsserts) {
        throw new AssertionError(message)
      } else {
        log.trace(s"assertion failed: ${message}")
      }
    }
  }
}

object MemsqlOptions extends LazyLogging {
  abstract class OptionEnum extends Enumeration {
    def fromString(s: String): Option[Value] =
      values.find(_.toString.toLowerCase() == s.toLowerCase())

    lazy val valuesString = values.mkString(", ")

    def fromOption(optionName: String, source: Option[String], default: Value): Value =
      source
        .orElse(Some(default.toString))
        .flatMap(fromString)
        .getOrElse(
          sys.error(
            s"Option '$optionName' must be one of the following values: ${valuesString}"
          )
        )
  }

  object LoadDataFormat extends OptionEnum {
    val CSV, Avro = Value
  }

  object CompressionType extends OptionEnum {
    val GZip, LZ4, Skip = Value
  }

  object TableKeyType extends OptionEnum {
    val Primary, Columnstore, Unique, Shard, Key = Value
  }

  case class TableKey(keyType: TableKeyType.Value,
                      name: Option[String] = None,
                      columns: String = "")

  private val memsqlOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    memsqlOptionNames += name.toLowerCase()
    name
  }

  final val DDL_ENDPOINT  = newOption("ddlEndpoint")
  final val DML_ENDPOINTS = newOption("dmlEndpoints")

  final val USER     = newOption("user")
  final val PASSWORD = newOption("password")

  final val DATABASE   = newOption("database")
  final val QUERY      = newOption("query")
  final val TABLE_NAME = newOption("dbtable")
  final val PATH       = newOption("path")

  // Write options
  final val TRUNCATE              = newOption("truncate")
  final val OVERWRITE_BEHAVIOR    = newOption("overwriteBehavior")
  final val LOAD_DATA_COMPRESSION = newOption("loadDataCompression")
  final val TABLE_KEYS            = newOption("tableKey")
  final val LOAD_DATA_FORMAT      = newOption("loadDataFormat")
  final val ON_DUPLICATE_KEY_SQL  = newOption("onDuplicateKeySQL")
  final val INSERT_BATCH_SIZE     = newOption("insertBatchSize")

  final val ENABLE_ASSERTS       = newOption("enableAsserts")
  final val DISABLE_PUSHDOWN     = newOption("disablePushdown")
  final val ENABLE_PARALLEL_READ = newOption("enableParallelRead")
  final val USE_EXTERNAL_HOST    = newOption("useExternalHost")

  def getTable(options: CaseInsensitiveMap[String]): Option[TableIdentifier] =
    options
      .get(TABLE_NAME)
      .orElse(options.get(PATH))
      .map(CatalystSqlParser.parseTableIdentifier)

  def getQuery(options: CaseInsensitiveMap[String]): String = {
    val table = getTable(options)

    require(
      !(table.isDefined && options.isDefinedAt(QUERY)),
      s"The '$QUERY' option cannot be specified along with a table name."
    )

    options
      .get(QUERY)
      .orElse(table.map(t => s"SELECT * FROM ${t.quotedString}"))
      .getOrElse(
        throw new IllegalArgumentException(
          s"One of the following options must be specified: $QUERY, $TABLE_NAME, $PATH"
        )
      )
  }

  def apply(options: CaseInsensitiveMap[String]): MemsqlOptions = {
    val table = getTable(options)

    require(options.isDefinedAt(DDL_ENDPOINT), s"Option '$DDL_ENDPOINT' is required.")

    val loadDataCompression = CompressionType.fromOption(LOAD_DATA_COMPRESSION,
                                                         options.get(LOAD_DATA_COMPRESSION),
                                                         CompressionType.GZip)

    // tableKeys are specified via options with the tableKey prefix
    // the key is `tableKey.TYPE.NAME` where
    //    TYPE is one of "primary, unique, shard, index"
    //    NAME is the index name - must be unique
    // the value is a column delimited list of columns for the index
    val tableKeys = options
      .filterKeys(_.toLowerCase.startsWith(TABLE_KEYS.toLowerCase + "."))
      .map {
        case (key, columns) => {
          val keyParts = key.split("\\.", 3)
          if (keyParts.length < 2) {
            sys.error(
              s"Options starting with '$TABLE_KEYS.' must be formatted correctly. The key should be in the form `$TABLE_KEYS.INDEX_TYPE[.NAME]`."
            )
          }
          val keyType = TableKeyType
            .fromString(keyParts(1))
            .getOrElse(
              sys.error(
                s"Option '$key' must specify an index type from the following options: ${TableKeyType.valuesString}")
            )
          val keyName = keyParts.lift(2)
          if (keyName.contains("")) {
            sys.error(s"Option '$key' can not have an empty name")
          }
          TableKey(keyType, keyName, columns)
        }
      }
      .toList

    new MemsqlOptions(
      ddlEndpoint = options(DDL_ENDPOINT),
      dmlEndpoints = options.getOrElse(DML_ENDPOINTS, options(DDL_ENDPOINT)).split(",").toList,
      user = options.getOrElse(USER, "root"),
      password = options.getOrElse(PASSWORD, ""),
      database = options.get(DATABASE).orElse(table.flatMap(t => t.database)),
      jdbcExtraOptions = options.originalMap
        .filterKeys(key => !memsqlOptionNames(key.toLowerCase()))
        // filterKeys produces a map which is not serializable due to a bug in Scala
        // https://github.com/scala/bug/issues/7005
        // mapping everything through the identity function fixes it...
        .map(identity),
      enableAsserts = options.get(ENABLE_ASSERTS).getOrElse("false").toBoolean,
      disablePushdown = options.get(DISABLE_PUSHDOWN).getOrElse("false").toBoolean,
      enableParallelRead = options.get(ENABLE_PARALLEL_READ).getOrElse("false").toBoolean,
      useExternalHost = options.get(USE_EXTERNAL_HOST).getOrElse("false").toBoolean,
      overwriteBehavior = {
        val truncateOption          = options.get(TRUNCATE)
        val overwriteBehaviorOption = options.get(OVERWRITE_BEHAVIOR)
        if (truncateOption.isDefined && overwriteBehaviorOption.isDefined) {
          throw new IllegalArgumentException(
            s"can't use both `$TRUNCATE` and `$OVERWRITE_BEHAVIOR` options, please use just `$OVERWRITE_BEHAVIOR` option instead.")
        }
        if (truncateOption.getOrElse("false").toBoolean) {
          log.warn(
            s"`$TRUNCATE` option is deprecated, please use the `$OVERWRITE_BEHAVIOR` option instead.")
          Truncate
        } else {
          /* DropAndCreate is the default behaviour if another isn't defined */
          overwriteBehaviorOption
            .fold[OverwriteBehavior](DropAndCreate)(OverwriteBehavior(_))
        }
      },
      loadDataCompression = loadDataCompression,
      loadDataFormat = LoadDataFormat.fromOption(LOAD_DATA_FORMAT,
                                                 options.get(LOAD_DATA_FORMAT),
                                                 LoadDataFormat.CSV),
      tableKeys = tableKeys,
      onDuplicateKeySQL = options.get(ON_DUPLICATE_KEY_SQL),
      insertBatchSize = options.get(INSERT_BATCH_SIZE).getOrElse("10000").toInt
    )
  }
}
