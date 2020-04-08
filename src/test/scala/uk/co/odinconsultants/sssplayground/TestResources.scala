package uk.co.odinconsultants.sssplayground

import java.io.File.separator
import java.io.{BufferedInputStream, FileInputStream, InputStream}

object TestResources {

  val EncryptedFileContents  = "Well done! You've cracked the code!\n"

  val PassPhrase: Array[Char] = "thisisatest".toCharArray

  def testResourceFQN(filename: String): String = {
    val tld = this.getClass.getResource(separator)
    s"${tld}..${separator}..${separator}src${separator}test${separator}resources${separator}$filename"
  }

  def filenameOf(resource: String): String = testResourceFQN(resource).substring(5)

  def pkInputStream(): BufferedInputStream =
    inputStreamFrom(filenameOf("alice_privKey.txt"))

  def inputStreamFrom(filename: String): BufferedInputStream = new BufferedInputStream(new FileInputStream(filename))

  val ZippedEncryptedFilename = "text.gpg.zip"
  val ZippedEncryptedFileFQN: String = testResourceFQN(ZippedEncryptedFilename)
}
