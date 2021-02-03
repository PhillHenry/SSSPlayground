package uk.co.odinconsultants.sssplayground.state

import java.sql.Timestamp

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}

object DedupeQuestionCode { // from https://ideone.com/nZ5pq2

  val EventTime = "eventTime"
  val UserId    = "userId"
  val Name      = "name"
  case class User(name: String, userId: Integer, age: Integer, eventTime: Timestamp)
  case class StateClass(totalUsers: Int, payload: String)

  val timeoutDuration = "2 seconds"

  def now(): Timestamp = new Timestamp(new java.util.Date().getTime)

  def mark(firstTime: Timestamp):   User = User("mark", 111, 21, firstTime)
  def john(firstTime: Timestamp):   User = User("john", 123, 22, firstTime)
  def sean(firstTime: Timestamp):   User = User("sean", 111, 22, firstTime)
  def robin(firstTime: Timestamp):  User = User("robin", 123, 23, firstTime)
  def stuart(firstTime: Timestamp): User = User("stuart", 14, 23, firstTime)
  def tom(firstTime: Timestamp):    User = User("tom", 111, 24, firstTime)
  def mike(firstTime: Timestamp):   User = User("mike", 123, 25, firstTime)

  def removeDuplicates(inputData: Dataset[User], spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    inputData
      .groupByKey(_.userId)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.ProcessingTimeTimeout)(removeDuplicatesInternal)
      .groupByKey(_.age)
      .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.ProcessingTimeTimeout)(anotherGroupWithState)
  }

  def anotherGroupWithState(id: Integer, newData: Iterator[User], state: GroupState[StateClass]): Iterator[User] = {
    val newDataList = newData.toList
    println(s"anotherGroupWithState id = $id, newDataList = $newDataList, state = $state")
    newDataList.iterator
  }

  def removeDuplicatesInternal(id: Integer, newData: Iterator[User], state: GroupState[StateClass]): Iterator[User] = {
    if (state.hasTimedOut) {
      println(s"State timed out: $state")
      state.remove()
      return Iterator()
    }
    if (newData.isEmpty) {
      println(s"New data is empty for state: $state")
      return Iterator()
    }

    if (!state.exists) {
      val firstUserData = newData.next()
      val newState = StateClass(1, s"${now()}-$id") // Total count = 1 initially
      state.update(newState)
      state.setTimeoutDuration(timeoutDuration)
      println(s"State does not exist. Creating: $state and returning $firstUserData")
      Iterator(firstUserData) // Returning UserData first time
    }
    else {
      val newState = StateClass(state.get.totalUsers + 1, s"${now()}-$id")
      state.update(newState)
      state.setTimeoutDuration(timeoutDuration)
      println(s"updating state: $state")
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

//    val batchStream = datasetStream.writeStream.foreachBatch { writeBatchFn }
//      .outputMode("update")
//      .start()
//      .awaitTermination(10000)

    val dedupedStream = removeDuplicates(datasetStream, spark)
    import org.apache.spark.sql.functions._

    dedupedStream
      .writeStream.format("console").outputMode(OutputMode.Append())
      .foreachBatch(showBatch)
      .start()

    val firstTime = now()
    val firstUsers = Seq(
      mark(firstTime),
      john(firstTime),
      sean(firstTime)
    )
    memoryStream.addData(firstUsers)

    Thread.sleep(3000)
    println("1st Batch over")

    memoryStream.addData(Seq(
      robin(firstTime),
      stuart(firstTime),
      tom(firstTime),
      mike(firstTime)
    ))
    Thread.sleep(3000)
    println("2nd Batch over")
    println("That's all folks. Any errors after this point are irrelevant")
  }

  val showBatchDF: (DataFrame, Long) => Unit = { case (batch, batchId) =>
    println(s"==================== Batch id $batchId ====================")
    batch.show()
  }
  val showBatch: (Dataset[User], Long) => Unit = { case (batch, batchId) =>
    println(s"Batch id $batchId")
    batch.show()
  }
}
