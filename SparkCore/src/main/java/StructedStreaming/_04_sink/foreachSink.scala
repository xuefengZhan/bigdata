package StructedStreaming._04_sink

import org.apache.spark.sql.ForeachWriter

object foreachSink extends ForeachWriter {
  override def open(partitionId: Long, epochId: Long): Boolean = ???

  override def process(value: Nothing): Unit = ???

  override def close(errorOrNull: Throwable): Unit = ???
}
