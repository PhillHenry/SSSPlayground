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
import uk.co.odinconsultants.sssplayground.TestResources
import uk.co.odinconsultants.sssplayground.TestResources.testResourceFQN

import scala.annotation.tailrec

class DecryptStreamSpec extends WordSpec with Matchers {

  import DecryptStreamSpec._

  def randomFilename(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  "Binary file in HDFS" should {
    val to        = randomFilename()
    Security.addProvider(new BouncyCastleProvider)
    "be decrypted by Spark" in {
      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(testResourceFQN("text.gpg")), new Path(to))
      sparkDecryption(decryptingFn, to) shouldBe expected
    }
    "be unzipped and decrypted by Spark" ignore {
      val toZipFile        = randomFilename()
      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(testResourceFQN("text.gpg.zip")), new Path(toZipFile))
      sparkDecryption(unzippingFn, toZipFile) shouldBe expected
    }
    "be decrypted using Hadoop's API" in {
      val is        = HdfsForTesting.distributedFS.open(new Path(to), 1024)
      val decrypted = decrypt(is, pkInputStream(), "thisisatest".toCharArray)
      decrypted shouldBe expected
    }
  }
}

object DecryptStreamSpec {

  type BinaryFilesFn = (String, PortableDataStream) => String

  val expected  = "Well done! You've cracked the code!\n"

  def decryptingFn(f: String, stream: PortableDataStream): String = {
    decrypt(stream.open(), pkInputStream(), "thisisatest".toCharArray)
  }

  def unzippingFn(f: String, stream: PortableDataStream): String = {
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

  def sparkDecryption(binaryReadingFn: BinaryFilesFn, to: String): String = {
    val decodedRDD = session.sparkContext.binaryFiles(to).map { case (f, stream) =>
      binaryReadingFn(f, stream)
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
    val pkFile: String = testResourceFQN("alice_privKey.txt").substring(5)
    new BufferedInputStream(new FileInputStream(pkFile))
  }


}
