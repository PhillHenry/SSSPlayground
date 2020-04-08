package uk.co.odinconsultants.sssplayground.security

import java.io._
import java.security.Security
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.hadoop.fs.Path
import org.apache.spark.input.PortableDataStream
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting.hdfsUri
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.sssplayground.TestResources._

import scala.annotation.tailrec

class SparkDecryptStreamSpec extends WordSpec with Matchers {

  import SparkDecryptStreamSpec._

  def randomFilename(): String = hdfsUri + this.getClass.getSimpleName + System.nanoTime()

  "Binary file in HDFS" should {
    val to        = randomFilename()
    Security.addProvider(new BouncyCastleProvider)
    "be decrypted by Spark" in {
      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(testResourceFQN("text.gpg")), new Path(to))
      sparkDecryption(decryptingFn, to) shouldBe EncryptedFileContents
    }
    "be unzipped and decrypted by Spark" ignore {
      val toZipFile        = randomFilename()

      HdfsForTesting.distributedFS.copyFromLocalFile(new Path(ZippedEncryptedFileFQN), new Path(toZipFile))
      sparkDecryption(unzippingFn, toZipFile) shouldBe EncryptedFileContents
    }
    "be decrypted using Hadoop's API" in {
      val is        = HdfsForTesting.distributedFS.open(new Path(to), 1024)
      val decrypted = decrypt(is, pkInputStream(), "thisisatest".toCharArray)
      decrypted shouldBe EncryptedFileContents
    }
  }
}

object SparkDecryptStreamSpec {

  type BinaryFilesFn = (String, PortableDataStream) => String

  def decryptingFn(f: String, stream: PortableDataStream): String = {
    decrypt(stream.open(), pkInputStream(), PassPhrase)
  }

  def unzippingFn(f: String, stream: PortableDataStream): String = {
    val s: DataInputStream = stream.open()
    val zipStream = new ZipInputStream(s)

    @tailrec
    def read(entry: ZipEntry, acc: String): String = {
      println("PH acc = " + acc)
      if (entry != null) {
        read(zipStream.getNextEntry, acc + decrypt(zipStream, pkInputStream(), PassPhrase))
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


}
