package org.apache.spark

import org.apache.spark.io.CompressionCodec
import uk.co.odinconsultants.htesting.spark.SparkForTesting.sparkConf

import java.io.DataInputStream

object CodecAccess {
  def decompressStream(inputStream: DataInputStream, compressionCodec: String): DataInputStream = {
    val compressed = CompressionCodec.createCodec(sparkConf, compressionCodec)
      .compressedInputStream(inputStream)
    new DataInputStream(compressed)
  }

}
