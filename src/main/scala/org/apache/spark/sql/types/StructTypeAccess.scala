package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.expressions.AttributeReference

object StructTypeAccess {

  def toAttribute(x: StructType): Seq[AttributeReference] = x.toAttributes

}
