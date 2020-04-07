package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, FileInputStream, InputStream}
import java.io.File.separator
import java.security.Security

import org.apache.hadoop.fs.Path
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class DecryptStreamSpec extends WordSpec with Matchers {

  import DecryptStreamSpec._

  "Binary file in HDFS" should {
    val expected  = "Well done! You've cracked the code!\n"
    val from      = testResourceFQN("text.gpg")
    val to        = hdfsUri + this.getClass.getSimpleName
    "be decrypted by Spark" in {
      Security.addProvider(new BouncyCastleProvider)

      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(from), new Path(to))

      val decodedRDD = session.sparkContext.binaryFiles(to).map { case (_, stream) =>
        val inputStream:      DataInputStream       = stream.open()
        decrypt(inputStream, pkInputStream(), "thisisatest".toCharArray)
      }
      val result = decodedRDD.collect()
      result(0) shouldBe expected
    }
    "be decrypted using Hadoop's API" in {
      val is        = HdfsForTesting.distributedFS.open(new Path(to), 1024)
      val decrypted = decrypt(is, pkInputStream(), "thisisatest".toCharArray)
      decrypted shouldBe expected
    }
  }
}

object DecryptStreamSpec {

  def decrypt(in: InputStream, keyIn: InputStream, passwd: Array[Char]): String = {
    val baos    = new ByteArrayOutputStream
    PGPDecryptor.decrypt(in, keyIn, passwd, baos)
    val result  = new String(baos.toByteArray)
    baos.close()
    result
  }

  def pkInputStream(): BufferedInputStream = {
    val pkFile: String = DecryptStreamSpec.testResourceFQN("alice_privKey.txt").substring(5)
    new BufferedInputStream(new FileInputStream(pkFile))
  }

  def testResourceFQN(filename: String): String = {
    val tld = this.getClass.getResource(separator)
    s"${tld}..${separator}..${separator}src${separator}test${separator}resources${separator}$filename"
  }

}
