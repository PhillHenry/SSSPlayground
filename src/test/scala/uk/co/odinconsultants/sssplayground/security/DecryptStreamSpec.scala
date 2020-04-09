package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayInputStream, ByteArrayOutputStream}

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.sssplayground.TestResources._

class DecryptStreamSpec extends WordSpec with Matchers {

  import DecryptStream._

  def nameToOut(baos1: ByteArrayOutputStream, baos2: ByteArrayOutputStream, name: String): ByteArrayOutputStream = {
    if (name == "text.gpg") {
      baos1
    } else if (name == "text2.gpg") {
      baos2
    } else throw new Exception(s"wasn't expecting file $name")
  }

  "Zipped, encrypted file" should {
    "be decrypted into 2 streams" in {
      val in: BufferedInputStream = inputStreamFrom(filenameOf(ZippedEncrypted2FilesFilename))
      val baos1 = new ByteArrayOutputStream()
      val baos2 = new ByteArrayOutputStream()
      val nameToOutFn: NameTo[ByteArrayOutputStream] = nameToOut(baos1, baos2, _)
      unzipping(pkInputStream(), PassPhrase, nameToOutFn, in)
      readAndCheck(baos1, EncryptedFileContents)
      readAndCheck(baos2, EncryptedFileContents2)
    }
    "be decrypted" in {
      val in: BufferedInputStream = inputStreamFrom(filenameOf(ZippedEncryptedFilename))
      val baos1 = new ByteArrayOutputStream()
      val baos2 = new ByteArrayOutputStream()
      val nameToOutFn: NameTo[ByteArrayOutputStream] = nameToOut(baos1, baos2, _)
      unzipping(pkInputStream(), PassPhrase, nameToOutFn, in)
      readAndCheck(baos1, EncryptedFileContents)
    }
  }

  def readAndCheck(baos: ByteArrayOutputStream, expected: String): Unit = {
    val result  = new String(baos.toByteArray)
    baos.close()
    baos.close()
    result shouldBe expected
  }

  "An input stream" should {
    "be converted to a byte array" in {
      val actual = "1234567890" * 100
      val bais = new ByteArrayInputStream(actual.getBytes())
      val out = readToByteArray(bais)
      new String(out) shouldBe actual
    }
  }

}
