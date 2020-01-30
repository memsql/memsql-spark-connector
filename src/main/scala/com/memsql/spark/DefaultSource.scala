package com.memsql.spark

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{
  BaseRelation,
  CreatableRelationProvider,
  DataSourceRegister,
  RelationProvider,
  SchemaRelationProvider
}
import org.apache.spark.sql.types.StructType

object DefaultSource {
  val MEMSQL_SOURCE_NAME          = "com.memsql.spark"
  val MEMSQL_SOURCE_NAME_SHORT    = "memsql"
  val MEMSQL_GLOBAL_OPTION_PREFIX = "spark.datasource.memsql."
}

class DefaultSource
    extends RelationProvider
    with DataSourceRegister
    with CreatableRelationProvider
    with LazyLogging {

  override def shortName: String = DefaultSource.MEMSQL_SOURCE_NAME_SHORT

  private def includeGlobalParams(sqlContext: SQLContext,
                                  params: Map[String, String]): Map[String, String] =
    sqlContext.getAllConfs.foldLeft(params)({
      case (params, (k, v)) if k.startsWith(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) =>
        params + (k.stripPrefix(DefaultSource.MEMSQL_GLOBAL_OPTION_PREFIX) -> v)
      case (params, _) => params
    })

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    SQLPushdownRule.ensureInjected(sqlContext.sparkSession)
    val opts = CaseInsensitiveMap(includeGlobalParams(sqlContext, parameters))
    MemsqlReader(opts, sqlContext)
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val opts = CaseInsensitiveMap(includeGlobalParams(sqlContext, parameters))
    val conf = MemsqlOptions(opts)

    val table = MemsqlOptions
      .getTable(opts)
      .getOrElse(
        throw new IllegalArgumentException(
          s"To write a dataframe to MemSQL you must specify a table name via the '${MemsqlOptions.TABLE_NAME}' parameter"
        )
      )

    JdbcHelpers.prepareTableForWrite(conf, table, mode, data.schema)

    val partitionWriterFactory = new PartitionWriterFactory(table, conf)
    data.foreachPartition(partition => {
      val writer = partitionWriterFactory.createDataWriter(TaskContext.getPartitionId(), 0)
      try {
        partition.foreach(writer.write(_))
        writer.commit()
      } catch {
        case e: Exception => {
          writer.abort()
          throw e
        }
      }
    })

    createRelation(sqlContext, parameters)
  }
}
