package com.singlestore.spark

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.singlestore.spark.SQLGen.VariableList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}

case class SinglestoreRDD(query: String,
                          variables: VariableList,
                          options: SinglestoreOptions,
                          schema: StructType,
                          expectedOutput: Seq[Attribute],
                          @transient val sc: SparkContext)
    extends RDD[Row](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    SinglestoreQueryHelpers.GetPartitions(options, query, variables)

  override def compute(rawPartition: Partition, context: TaskContext): Iterator[Row] = {
    var closed                          = false
    var rs: ResultSet                   = null
    var stmt: PreparedStatement         = null
    var conn: Connection                = null
    var partition: SinglestorePartition = rawPartition.asInstanceOf[SinglestorePartition]

    def tryClose(name: String, what: AutoCloseable): Unit = {
      try {
        if (what != null) { what.close() }
      } catch {
        case e: Exception => logWarning(s"Exception closing $name", e)
      }
    }

    def close(): Unit = {
      if (closed) { return }
      tryClose("resultset", rs)
      tryClose("statement", stmt)
      tryClose("connection", conn)
      closed = true
    }

    context.addTaskCompletionListener {
      new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = close()
      }
    }

    conn = SinglestoreConnectionFactory.getConnection(partition.connectionInfo)
    stmt = conn.prepareStatement(partition.query)
    JdbcHelpers.fillStatement(stmt, partition.variables)
    rs = stmt.executeQuery()

    var rowsIter = JdbcUtils.resultSetToRows(rs, schema)

    if (expectedOutput.nonEmpty) {
      val schemaDatatypes   = schema.map(_.dataType)
      val expectedDatatypes = expectedOutput.map(_.dataType)

      def getOrNull(f: => Any, r: Row, i: Int): Any = {
        if (r.isNullAt(i)) null
        else f
      }

      if (schemaDatatypes != expectedDatatypes) {
        val columnEncoders = schemaDatatypes.zip(expectedDatatypes).zipWithIndex.map {
          case ((_: StringType, _: NullType), _) => ((_: Row) => null)
          case ((_: ShortType, _: BooleanType), i) =>
            (r: Row) =>
              getOrNull(r.getShort(i) != 0, r, i)
          case ((_: IntegerType, _: BooleanType), i) =>
            (r: Row) =>
              getOrNull(r.getInt(i) != 0, r, i)
          case ((_: LongType, _: BooleanType), i) =>
            (r: Row) =>
              getOrNull(r.getLong(i) != 0, r, i)

          case ((_: ShortType, _: ByteType), i) =>
            (r: Row) =>
              getOrNull(r.getShort(i).toByte, r, i)
          case ((_: IntegerType, _: ByteType), i) =>
            (r: Row) =>
              getOrNull(r.getInt(i).toByte, r, i)
          case ((_: LongType, _: ByteType), i) =>
            (r: Row) =>
              getOrNull(r.getLong(i).toByte, r, i)

          case ((l, r), i) =>
            options.assert(l == r, s"SinglestoreRDD: unable to encode ${l} into ${r}")
            ((r: Row) => getOrNull(r.get(i), r, i))
        }

        rowsIter = rowsIter
          .map(row => Row.fromSeq(columnEncoders.map(_(row))))
      }
    }

    CompletionIterator[Row, Iterator[Row]](new InterruptibleIterator[Row](context, rowsIter), close)
  }

}
