package uk.co.odinconsultants.sssplayground.security

import java.io.{DataInputStream, InputStream, OutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.spark.input.PortableDataStream

import scala.annotation.tailrec

object DecryptStream {

  def unzipping(pkInputStream:  InputStream,
                pass:           String,
                out:            OutputStream,
                stream:         PortableDataStream): Unit = {
    val s: DataInputStream = stream.open()
    val zipStream = new ZipInputStream(s)

    @tailrec
    def read(entry: ZipEntry): Unit = {
      if (entry != null) {
        PGPDecryptor.decrypt(zipStream, pkInputStream, pass.toCharArray, out)
        read(zipStream.getNextEntry)
      }
    }
    read(zipStream.getNextEntry) // TODO
  }

  def pipe(in: InputStream, out: OutputStream): Unit = { // TODO
    ???
  }
}
