package uk.co.odinconsultants.sssplayground.security

import java.security.SecureRandom
import java.util.Base64

object SecureRandomDemoMain {

  def main(args: Array[String]): Unit = {
    val random  = new SecureRandom()
    val nBytes  = 16
    val buffer  = Array.ofDim[Byte](nBytes)
    val acc     = Array.fill(nBytes)(0.toByte)
    val n       = math.pow(10, 8).toInt
    val start   = System.currentTimeMillis()
    (1 to n).foreach { _ =>
      random.nextBytes(buffer)
      buffer.zipWithIndex.map{ case (b, i) => acc(i) = (acc(i) ^ b).toByte } // to stop the JVM optimizing the loop away
    }
    val end = System.currentTimeMillis()

    println(s"\n${new String(Base64.getEncoder.encode(acc))}")
    println(s"\nGenerating $n byte arrays of size $nBytes took ${(end - start) / 1000d}s")
  }

}
