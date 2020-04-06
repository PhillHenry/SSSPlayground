package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayInputStream, DataInputStream, FileInputStream}
import java.io.File.separator
import java.security.Security

import org.apache.hadoop.fs.Path
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

class DecryptStreamSpec extends WordSpec with Matchers {

  "Binary file in HDFS" should {
    "be decrypted" in {
      Security.addProvider(new BouncyCastleProvider)

      val from  = DecryptStreamSpec.testResourceFQN("text.gpg")
      val to    = hdfsUri + this.getClass.getSimpleName
      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(from), new Path(to))

      val decodedRDD = session.sparkContext.binaryFiles(to).map { case (file, stream) =>
        val inputStream:      DataInputStream       = stream.open()
        val pkFile:           String                = DecryptStreamSpec.testResourceFQN("alice_privKey.txt").substring(5)
        val privateKeyStream: BufferedInputStream   = new BufferedInputStream(new FileInputStream(pkFile))
        PGPDecryptor.decryptFile(inputStream, privateKeyStream, "thisisatest".toCharArray)
      }
      val result = decodedRDD.collect()
      result(0) shouldBe "Well done! You've cracked the code!\n"
    }
  }


}

object DecryptStreamSpec {

  def testResourceFQN(filename: String): String = {
    val tld = this.getClass.getResource(separator)
    s"${tld}..${separator}..${separator}src${separator}test${separator}resources${separator}$filename"
  }

}
