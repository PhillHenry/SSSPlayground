package uk.co.odinconsultants.sssplayground

import java.io.File.separator
import java.io.{BufferedInputStream, FileInputStream, InputStream}

object TestResources {

  def testResourceFQN(filename: String): String = {
    val tld = this.getClass.getResource(separator)
    s"${tld}..${separator}..${separator}src${separator}test${separator}resources${separator}$filename"
  }

  def filenameOf(resource: String): String = testResourceFQN(resource).substring(5)

  def inputStreamFrom(filename: String): BufferedInputStream = {
    println(s"Streaming from $filename")
    new BufferedInputStream(new FileInputStream(filename))
  }

}
