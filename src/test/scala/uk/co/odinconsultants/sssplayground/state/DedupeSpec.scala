package uk.co.odinconsultants.sssplayground.state

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class DedupeSpec extends WordSpec with Matchers {

  import Dedupe._

  "De-duping" should {
    import session.implicits._
    "remove dupes from across batches" in {
      val ids       = Seq(1, 3, 5, 1)
      val xs        = ids.map(x => User("a name", x))
      val df        = xs.toDS
      val deduped   = removeDuplicates(df).collect()
      deduped.map(_.userId).toSet shouldEqual ids.toSet
    }
  }

}
