package sjtu.project

import java.io.{File, FileInputStream, IOException, InputStream}
import java.security.MessageDigest

import scala.collection.mutable.ArrayBuffer

object MD5Util {
  val encodingAlgorithm = "MD5"

  def convertToHexString(data: Array[Byte]): String = {
    val strBuffer = new StringBuffer()
    for (i <- 0 until data.length) {
      strBuffer.append((0xff & data(i)).toHexString)
    }
    strBuffer.toString
  }

  def md5sum(file: File): Array[Byte] = {
    var fis: InputStream = null
    val buffer = new Array[Byte](1024)
    var checkSum: MessageDigest = null
    try {
      fis = new FileInputStream(file)
      checkSum = MessageDigest.getInstance(encodingAlgorithm)
      var numRead: Int = fis.read(buffer)
      while (numRead > 0) {
        checkSum.update(buffer, 0, numRead)
        numRead = fis.read(buffer)
      }
      return checkSum.digest()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (fis != null) {
        try {
          fis.close()
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
    null
  }

  def md5sum(filelist: ArrayBuffer[File]): Array[Byte] = {
    var fis: InputStream = null
    val buffer = new Array[Byte](1024)
    var checkSum: MessageDigest = null
    try {
      checkSum = MessageDigest.getInstance(encodingAlgorithm)
      filelist.foreach(file => {
        try{
          fis = new FileInputStream(file)
          var numRead = fis.read(buffer)
          while (numRead > 0) {
            checkSum.update(buffer, 0, numRead)
            numRead = fis.read(buffer)
          }
        } catch {
          case e: IOException => e.printStackTrace()
        } finally {
          if (fis != null) {
            try {
              fis.close()
            } catch {
              case e: IOException => e.printStackTrace()
            }
          }
        }
      })
      return checkSum.digest()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    null
  }

}
