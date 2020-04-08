package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayOutputStream}

import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.sssplayground.TestResources._

class DecryptStreamSpec extends WordSpec with Matchers {

  import DecryptStream._

  "Zipped, encrypted file" should {
    "be decrypted" in {
      val baos = new ByteArrayOutputStream()
      val in: BufferedInputStream = inputStreamFrom(filenameOf(ZippedEncryptedFilename))
      unzipping(pkInputStream(), PassPhrase, baos, in)
      val result  = new String(baos.toByteArray)
      baos.close()
      result shouldBe EncryptedFileContents
    }
  }

}
