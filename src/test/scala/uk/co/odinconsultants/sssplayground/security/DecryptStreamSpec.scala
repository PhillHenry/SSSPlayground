package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayInputStream, ByteArrayOutputStream}

import uk.co.odinconsultants.sssplayground.TestResources._
import zio.test.Assertion.equalTo
import zio.test.environment.TestEnvironment
import zio.test._
import zio.{Task, ZIO}

object DecryptStreamSpec extends DefaultRunnableSpec {

  import DecryptStream._

  def nameToOut(baos1: ByteArrayOutputStream, baos2: ByteArrayOutputStream, name: String): ByteArrayOutputStream = {
    if (name == "text.gpg") {
      baos1
    } else if (name == "text2.gpg") {
      baos2
    } else throw new Exception(s"wasn't expecting file $name")
  }

  def zioUnzip(file: String): Task[(ByteArrayOutputStream, ByteArrayOutputStream)] = ZIO {
    val in: BufferedInputStream = inputStreamFrom(filenameOf(file))
    val baos1 = new ByteArrayOutputStream()
    val baos2 = new ByteArrayOutputStream()
    val nameToOutFn: NameTo[ByteArrayOutputStream] = nameToOut(baos1, baos2, _)
    unzipping(pkInputStream(), PassPhrase, nameToOutFn, in)
    (baos1, baos2)
  }

  override def spec: ZSpec[TestEnvironment, Any] = suite("Zipping and encrypting legacy stream")(
    testM("decrypt into 2 streams"){
       zioUnzip(ZippedEncrypted2FilesFilename).flatMap { case (baos1, baos2) =>
         readAndCheck(baos1, EncryptedFileContents) *> readAndCheck(baos2, EncryptedFileContents2)
       }
    }
    ,
    testM("decrypted") {
      zioUnzip(ZippedEncryptedFilename).flatMap { case (baos1, baos2) =>
        readAndCheck(baos1, EncryptedFileContents) *> readAndCheck(baos2, "")
      }
    }
    ,
    testM("a legacy input stream should be converted to a byte array") {
      val actual  = "1234567890" * 100
      ZIO {
        val bais    = new ByteArrayInputStream(actual.getBytes())
        val out     = readToByteArray(bais)
        bais.close()
        out
      }.flatMap { out =>
        val shouldSatisfy:  Assertion[String] => TestResult = assert(new String(out))
        val assertion:      Assertion[Any]                  = equalTo(actual)
        ZIO(shouldSatisfy(assertion))
      }
    }
  )

  def readAndCheck(baos: ByteArrayOutputStream, expected: String): Task[TestResult] =
    ZIO {
      val result  = new String(baos.toByteArray)
      baos.close()
      result
    }.flatMap { result =>
      val shouldSatisfy:  Assertion[String] => TestResult = assert(result)
      val assertion:      Assertion[Any]                  = equalTo(expected)
      ZIO(shouldSatisfy(assertion))
    }


}
