package uk.co.odinconsultants.sssplayground.state

import java.sql.Timestamp

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import uk.co.odinconsultants.sssplayground.joins.RunningAverageMain.{Datum, DatumDelimiter}
import uk.co.odinconsultants.sssplayground.kafka.Consuming.KafkaParseFn

/**
 * See https://stackoverflow.com/questions/65604303/using-flatmapgroupswithstate-with-foreachbatch
 */
object Dedupe {

  val writeBatch: (Dataset[User], Long) => Unit = { case (batch, batchId) =>
    val distinctBatch = removeDuplicates(batch)
  }

  case class User(name: String, userId: Integer)
  case class StateClass(totalUsers: Int)

  val parsingUser: KafkaParseFn[User] = { case (k, v) =>
    Some(User(k, v.toInt))
  }

  def removeDuplicates(inputData: Dataset[User]): Dataset[User] = {
    import inputData.sparkSession.implicits._
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
}
