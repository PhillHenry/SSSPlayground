package uk.co.odinconsultants.sssplayground.state

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

object DedupeQuestionCode { // from https://ideone.com/nZ5pq2

  case class User(name: String, userId: Integer)
  case class StateClass(totalUsers: Int)

  def removeDuplicates(inputData: Dataset[User], spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    inputData
      .groupByKey(_.userId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.ProcessingTimeTimeout)(removeDuplicatesInternal)
  }

  def removeDuplicatesInternal(id: Integer, newData: Iterator[User], state: GroupState[StateClass]): Iterator[User] = {
    if (state.hasTimedOut) {
      state.remove() // Removing state since no same UserId in 4 hours
      return Iterator()
    }
    if (newData.isEmpty)
      return Iterator()

    if (!state.exists) {
      val firstUserData = newData.next()
      val newState = StateClass(1) // Total count = 1 initially
      state.update(newState)
      state.setTimeoutDuration("4 hours")
      Iterator(firstUserData) // Returning UserData first time
    }
    else {
      val newState = StateClass(state.get.totalUsers + 1)
      state.update(newState)
      state.setTimeoutDuration("4 hours")
      Iterator() // Returning empty since state already exists (Already sent this UserData before)
    }
  }

  def writeBatch(batchDF: Dataset[User], spark: SparkSession): Unit = {
    batchDF.show(false)
    val distinctUserData = removeDuplicates(batchDF, spark)
    println("Distinct User Data")
    distinctUserData.show(false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.driver.memory", "5g")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    implicit val sqlCtx: SQLContext = spark.sqlContext
    import spark.implicits._

    val memoryStream = MemoryStream[User]
    val datasetStream = memoryStream.toDS()

    val writeBatchFn: (Dataset[User], Long) => Unit = { case (batch, batchId) =>
      writeBatch(batch, spark)
    }

    val batchStream = datasetStream.writeStream.foreachBatch { writeBatchFn }
      .outputMode("update")
      .start()
      .awaitTermination(10000)

    //    val dedupedStream = removeDuplicates(datasetStream, spark)
    //    dedupedStream.writeStream.format("console").outputMode("append").start()

    memoryStream.addData(Seq(
      User("mark", 111),
      User("john", 123),
      User("sean", 111)
    ))

    Thread.sleep(3000)
    println("1st Batch over")

    memoryStream.addData(Seq(
      User("robin", 123),
      User("stuart", 14),
      User("tom", 111),
      User("mike", 123)
    ))
    Thread.sleep(3000)
    println("2nd Batch over")
  }
}
