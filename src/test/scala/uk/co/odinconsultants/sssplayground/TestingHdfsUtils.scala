package uk.co.odinconsultants.sssplayground

import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.{list, readAsString}
import uk.co.odinconsultants.htesting.log.Logging

object TestingHdfsUtils extends Logging {

  def readFileFrom(dir: String, endingWith: String): Map[String, String] = {
    val files = list(dir).filter(_.toString.endsWith(endingWith))
    info(s"JSON files: ${files.mkString(", ")}")
    files.map { f =>
      f.toString -> readAsString(f.toString)
    }.toMap
  }

}
