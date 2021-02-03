package uk.co.odinconsultants.sssplayground.state

import com.google.common.io.ByteStreams
import org.apache.hadoop.fs.Path
import org.apache.spark.CodecAccess
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, encoderFor}
import org.apache.spark.sql.catalyst.expressions.{BoundReference, CaseWhen, Expression, GetStructField, IsNull, Literal, UnsafeRow}
import org.apache.spark.sql.execution.ObjectOperator
import org.apache.spark.sql.execution.streaming.state.{HDFSBackedStateStoreProviderAccess, StateStore}
import org.apache.spark.sql.execution.streaming.{FileSystemBasedCheckpointFileManager, MemoryStream}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, StructTypeAccess}
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.distributedFS
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestUtils
import uk.co.odinconsultants.sssplayground.state.DedupeQuestionCode._

import java.io.{DataInputStream, FileNotFoundException, IOException}
import scala.util.{Failure, Success, Try}

class FlatMapGroupsWithStateSpec extends WordSpec with Matchers {

  "Streams" should {
    "show user" ignore new TestUtils {
      import session.implicits._
      implicit val sqlCtx: SQLContext = session.sqlContext
      val path          = randomFileName()
      val memoryStream  = MemoryStream[User]
      val deduped       = removeDuplicates(memoryStream.toDS(), session)
      val stream2: StreamingQuery = deduped.writeStream
        .format("console")
        .outputMode("append")
        .option("checkpointLocation", path)
        .start()

      sendUsers(memoryStream)
      pause()

      sendUsers(memoryStream)
      pause()

      val dir = s"$path/state/"
      val statePath = new Path(dir)

      val manager = new FileSystemBasedCheckpointFileManager(statePath, HdfsForTesting.conf)
      manager.list(new Path(path)).foreach(x => println(s"file = $x"))

      HdfsForTesting.list(dir).foreach { p =>
        attemptRead(p, manager) match {
          case Success(_) =>
          case Failure(x) => println(s"Could not read $p")
        }
      }
//      examineStore(dir)
      memoryStream.stop()
      stream2.stop()
    }
  }

  def pause(): Unit = {
    println("Sleeping...")
    Thread.sleep(5000)
  }

  private def sendUsers(memoryStream: MemoryStream[User]) = {
    for (_ <- 1 to 1000) {
      memoryStream.addData(Seq(mark(now())))
//      memoryStream.addData(Seq(john(now())))
    }
  }

  def attemptRead(p: Path, manager: FileSystemBasedCheckpointFileManager) = Try {
    val status = distributedFS.listStatus(p)
    assert(status.length == 1)
    val s = status.head
    println(s"list path $p")
    Try {
      updateFromDeltaFile(p, manager)
    } match {
      case Success(x) => println(s"Read $x bytes of $p of ${s.getLen}")
      case Failure(exception) => println(s"Failed with ${exception.getMessage}")
    }
  }

  private def examineStore(dir: String) = {
    val sqlConf: SQLConf = getDefaultSQLConf(0, 10)
    val stateStoreProvider = HDFSBackedStateStoreProviderAccess.provider(HdfsForTesting.conf, dir, 1, 1, sqlConf)
    val store: StateStore = stateStoreProvider.getStore(2)
    println("store = " + store)
    val unsafeRow = new UnsafeRow(2)
    println("range = " + store.get(unsafeRow))
    println(s"unsafeRow = $unsafeRow")
  }

  def getDefaultSQLConf(minDeltasForSnapshot: Int, numOfVersToRetainInMemory: Int): SQLConf = {
    val sqlConf = new SQLConf()
    sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, minDeltasForSnapshot)
    sqlConf.setConf(SQLConf.MAX_BATCHES_TO_RETAIN_IN_MEMORY, numOfVersToRetainInMemory)
    sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 2)
    //    sqlConf.setConf(SQLConf.STATE_STORE_COMPRESSION_CODEC, SQLConf.get.stateStoreCompressionCodec)
    sqlConf
  }

  /**
   * Adapted from HDFSBackedStateStoreProvider.updateFromDeltaFile
   */
  private def updateFromDeltaFile(fileToRead: Path, fm: FileSystemBasedCheckpointFileManager): Long = {
    var input: DataInputStream = null
    val sourceStream = try {
      fm.open(fileToRead)
    } catch {
      case f: FileNotFoundException =>
        throw new IllegalStateException(
          s"Error reading delta file $fileToRead of $this: $fileToRead does not exist", f)
    }
    var bytesRead = 0L
    try {
      input = CodecAccess.decompressStream(sourceStream, "lz4") // snappy -> "FAILED_TO_UNCOMPRESS"
      var eof = false

      while(!eof) {
        val keySize = input.readInt()
        bytesRead += keySize
        if (keySize == -1) {
          eof = true
        } else if (keySize < 0) {
          throw new IOException(
            s"Error reading delta file $fileToRead of $this: key size cannot be $keySize")
        } else {
          val keyRowBuffer = new Array[Byte](keySize)
          ByteStreams.readFully(input, keyRowBuffer, 0, keySize)

          val keyRow = new UnsafeRow(1)
          keyRow.pointTo(keyRowBuffer, keySize)

          val valueSize = input.readInt()
          if (valueSize < 0) {
            println(s"remove($keyRow) in $fileToRead")
          } else {
            val stateDeserializerExpr = stateDeserializerExprAdapted(session)
            val stateDeserializerFunc =
              ObjectOperator.deserializeRowToObject(stateDeserializerExpr, StructTypeAccess.toAttribute(stateSchema))
            val valueRowBuffer = new Array[Byte](valueSize)
            ByteStreams.readFully(input, valueRowBuffer, 0, valueSize)
            val valueRow = new UnsafeRow(1)
            val deserialized = stateDeserializerFunc(valueRow)
            // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
            // This is a workaround for the following:
            // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
            // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
            valueRow.pointTo(valueRowBuffer, (valueSize / 8) * 8)
            println(s"map.put(key = $keyRow,  value = $valueRow) [$deserialized] in $fileToRead")
          }
        }
        keySize
      }
    } finally {
      if (input != null) input.close()
    }
    bytesRead
  }

  val stateSchema: StructType = StructType(Seq(StructField("totalUsers", IntegerType, true), StructField("payload", StringType, true)))

  /**
   * From StateManagerImplBase.stateDeserializerExpr
   */
  def stateDeserializerExprAdapted(spark: SparkSession): Expression = {
    import spark.implicits._
    // Note that this must be done in the driver, as resolving and binding of deserializer
    // expressions to the encoded type can be safely done only in the driver.
    val boundRefToNestedState = BoundReference(0, stateSchema, nullable = true)
    val encoder = encoderFor[StateClass]
    val stateEncoder = encoder.asInstanceOf[ExpressionEncoder[Any]]
    val deserExpr = stateEncoder.resolveAndBind().deserializer.transformUp {
      case BoundReference(ordinal, _, _) => GetStructField(boundRefToNestedState, ordinal)
    }
    val nullLiteral = Literal(null, deserExpr.dataType)
    CaseWhen(Seq(IsNull(boundRefToNestedState) -> nullLiteral), elseValue = deserExpr)
  }
}
