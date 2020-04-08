package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, FileInputStream, InputStream}
import java.io.File.separator
import java.security.Security
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.hadoop.fs.Path
import org.apache.spark.input.PortableDataStream
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session

import scala.annotation.tailrec

class DecryptStreamSpec extends WordSpec with Matchers {

  import DecryptStreamSpec._

  def randomFilename(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  "Binary file in HDFS" should {
    val to        = randomFilename()
    Security.addProvider(new BouncyCastleProvider)
    "be decrypted by Spark" in {
      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(testResourceFQN("text.gpg")), new Path(to))

      val decodedRDD = session.sparkContext.binaryFiles(to).map { case (_, stream) =>
        decrypt(stream.open(), pkInputStream(), "thisisatest".toCharArray)
      }
      val result = decodedRDD.collect()
      result(0) shouldBe expected
    }
    "be unzipped and decrypted by Spark" ignore {
      val toZipFile        = randomFilename()
      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(testResourceFQN("text.gpg.zip")), new Path(toZipFile))

      decryptAndCheck(fn, toZipFile) shouldBe expected
    }
    "be decrypted using Hadoop's API" in {
      val is        = HdfsForTesting.distributedFS.open(new Path(to), 1024)
      val decrypted = decrypt(is, pkInputStream(), "thisisatest".toCharArray)
      decrypted shouldBe expected
    }
  }
}

object DecryptStreamSpec {

  import DecryptStream._

  type BinaryFilesFn = (String, PortableDataStream) => String

  val expected  = "Well done! You've cracked the code!\n"

  def fn(f: String, stream: PortableDataStream): String = {
    val s: DataInputStream = stream.open()
    val zipStream = new ZipInputStream(s)

    @tailrec
    def read(entry: ZipEntry, acc: String): String = {
      println("PH acc = " + acc)
      if (entry != null) {
        read(zipStream.getNextEntry, acc + decrypt(zipStream, pkInputStream(), "thisisatest".toCharArray))
      } else {
        acc
      }
    }
    read(zipStream.getNextEntry, "")
  }

  def decryptAndCheck(fn: BinaryFilesFn, to: String): Unit = {
    val decodedRDD = session.sparkContext.binaryFiles(to).map { case (f, stream) =>
      fn(f, stream)
    }
    val result = decodedRDD.collect()
    result(0)
  }

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
