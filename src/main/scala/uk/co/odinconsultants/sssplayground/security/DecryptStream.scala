package uk.co.odinconsultants.sssplayground.security

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream, OutputStream}
import java.util.zip.{ZipEntry, ZipInputStream}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object DecryptStream {

  type NameTo[T] = String => T

  def unzipping[T <: OutputStream](pkInputStream:  InputStream,
                                   pass:           Array[Char],
                                   out:            NameTo[T],
                                   s:              InputStream): Unit = {
    val zipStream = new ZipInputStream(s)
    val pk        = readToByteArray(pkInputStream)

    @tailrec
    def read(entry: ZipEntry): Unit = {
      if (entry != null) {
        val buffer  = new Array[Byte](entry.getSize.toInt)
        zipStream.read(buffer)
        PGPDecryptor.decrypt(new ByteArrayInputStream(buffer), new ByteArrayInputStream(pk), pass, out(entry.getName))
        read(zipStream.getNextEntry)
      }
    }
    read(zipStream.getNextEntry)
  }

  def readToByteArray(in: InputStream, bufferSize: Int = 1024): Array[Byte] = {
    @tailrec
    def append(i: InputStream, stringBuffer: ArrayBuffer[Byte]): ArrayBuffer[Byte] = {
      val byteBuf = new Array[Byte](bufferSize)
      val nRead = i.read(byteBuf)
      if (nRead < 0) {
        stringBuffer
      } else {
        stringBuffer.appendAll(byteBuf.slice(0, nRead))
        append(i, stringBuffer)
      }
    }
    val read = append(in, new ArrayBuffer).toArray
    in.close()
    read
  }

}
