package uk.co.odinconsultants.sssplayground.delta

import uk.co.odinconsultants.sssplayground.delta.DatasetInspections.readCached

object DeltaLakeRead {

  def main(args: Array[String]): Unit = readCached(args(0))

}
