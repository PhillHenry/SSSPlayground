package uk.co.odinconsultants.sssplayground.security

import java.io.{DataInputStream, InputStream, OutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.spark.input.PortableDataStream

import scala.annotation.tailrec

object DecryptStream {

  def unzipping(pkInputStream:  InputStream,
                pass:           Array[Char],
                out:            OutputStream,
                s:              InputStream): Unit = {
    val zipStream = new ZipInputStream(s)

    @tailrec
    def read(entry: ZipEntry): Unit = {
      if (entry != null) {
        PGPDecryptor.decrypt(zipStream, pkInputStream, pass, out)
        read(zipStream.getNextEntry)
      }
    }
    read(zipStream.getNextEntry) // TODO
  }

  def pipe(in: InputStream, out: OutputStream): Unit = { // TODO
    ???
  }
}
