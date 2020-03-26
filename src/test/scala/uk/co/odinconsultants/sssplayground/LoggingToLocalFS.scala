package uk.co.odinconsultants.sssplayground

import java.io.{File, FileOutputStream}

import scala.util.Try

trait LoggingToLocalFS {

  def logToDisk(f2j: Map[String, String], dir: String): Unit = Try {
    val fqn = s"/tmp/${this.getClass.getSimpleName}/$dir/"
    new File(fqn).mkdirs()
    f2j.foreach { case (file, jsonString) =>
      val fileName  = file.substring(file.lastIndexOf("/"))
      val f         = new File(fqn + fileName)
      val fos       = new FileOutputStream(f)
      fos.write(jsonString.getBytes)
      fos.flush()
      fos.close()
    }
  }

}
