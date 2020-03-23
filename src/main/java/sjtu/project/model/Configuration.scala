package sjtu.project.model

import scala.collection.mutable.ArrayBuffer

object Configuration {
  val LENGTHOFBLOCK: Long = 2 * 1024 * 1024
  val DATASERVERNUM: Int = 4
  val NAMESERVERNUM: Int = 1

  val ORIGIN_FILELIST = "origin_fileList.txt"
  val DELETE_FILELIST = "delete_fileList.txt"
  val RECOVERY_FILELIST = "recovery_fileList.txt"

  val fileSeperator = "/" // for unix
//  val fileSeperator = "\\" // for windows
  val ROOT_DIR = "." + fileSeperator
  val DATASERVER_PREFIX = "DataServer" + fileSeperator

  val DATASERVER_DIRS = ArrayBuffer[String]()
  for (i <- 0 until DATASERVERNUM){
    DATASERVER_DIRS.append(DATASERVER_PREFIX + i.toString + fileSeperator)
  }
}
