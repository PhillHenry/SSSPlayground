package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object HDFSBackedStateStoreProviderAccess {
  val keySchema = StructType(Seq(StructField("key", StringType, true)))
  val valueSchema = StructType(Seq(StructField("value", IntegerType, true)))
  def provider(hadoopConf: Configuration,
               dir: String,
               opId: Long,
               partition: Int,
               sqlConf: SQLConf) = {
    val provider = new HDFSBackedStateStoreProvider()
    provider.init(
      StateStoreId(dir, opId, partition),
      keySchema,
      valueSchema,
      indexOrdinal = None,
      new StateStoreConf(sqlConf),
      hadoopConf)
    provider
  }
}
