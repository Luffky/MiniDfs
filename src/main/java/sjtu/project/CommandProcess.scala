package sjtu.project

import java.io._
import java.util.regex.Pattern

import javax.swing.JFileChooser
import sjtu.project.model.Configuration.fileSeperator
import sjtu.project.model.{Configuration, Task, TaskType}

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn


object CommandProcess {
  var isExit: Boolean = false
  val pattern = Pattern.compile("""\d+""")

  def Process(): Unit = this.synchronized {
    while (!isExit) {
      print(">> ")
      var cmd = StdIn.readLine()

      if (cmd.length > 0){
        val cmds = cmd.split(" ")
        val op = cmds(0)
        val length = cmds.length

        op match {
          case "write" => {
           if (length > 1) {
             upload(cmds(1))
           } else {
             println(MiniDFS.CurrentTime() + " ERROR: Lack params filePath")
           }
          }
          case "read" => {
            if (length > 1) {
              read(cmds)
            } else {
              println(MiniDFS.CurrentTime() + " ERROR: Lack params filePath")
            }
          }
          case "recovery" => {
            recovery()
          }
          case "delete" => {
            if (length > 1) {
              val ds_num = cmds(1)
              if (isInteger(ds_num)) {
                val num = ds_num.toInt
                if (num < Configuration.DATASERVERNUM) {
                  deleteDataServer(num)
                } else {
                  println(MiniDFS.CurrentTime() + " ERROR: DataServer's index should be larger than " + (num - 1))
                }
              } else {
                println(MiniDFS.CurrentTime() + " ERROR: second param should be a number")
              }
            } else {
              println(MiniDFS.CurrentTime() + " ERROR: Lack Params DataServer's index")
            }
          }
          case "ls" => {
            showFileStorage()
          }
          case "help" => {
            MiniDFS.help()
          }
          case "exit" => {
            exit()
          }
          case somethingElse => {
            println(MiniDFS.CurrentTime() + "ERROR: " + somethingElse + " is not a valid command")
          }
        }

        try {
          Thread.sleep(1000)
        } catch {
          case e:InterruptedException => {
            e.printStackTrace()
          }
        }


      }
    }
  }

  def upload(filePath: String): Unit = {
    var file: File = null
    if (filePath.equals("choose")) {
      file = getUploadFile()
      if (!conflict(file)) {
        MiniDFS.queueForNameServer.add(Task(TaskType.WRITE, file, "", 0, 0))
      } else {
        println(MiniDFS.CurrentTime() + " ERROR: another file with the same name")
      }
    } else {
      file = new File(filePath)
      if (file.exists() && file.isFile && !conflict(file)) {
        MiniDFS.queueForNameServer.add(model.Task(TaskType.WRITE, file, "", 0, 0))
      } else if (!file.exists()) {
        println(MiniDFS.CurrentTime() + " ERROR: file not exists")
      } else if (!file.isFile) {
        println(MiniDFS.CurrentTime() + " ERROR: it is not a file")
      } else {
        println(MiniDFS.CurrentTime() + " ERROR: there is another file with the same name")
      }
    }
  }

  def read(cmds: Array[String]): Unit = {
    val f = new File(cmds(1))
    val fileLength = getLength(f)
    if (fileLength == 0) {
      println(MiniDFS.CurrentTime() + " ERROR: file not exist")
    } else {
      if (cmds.length == 2) {
        MiniDFS.queueForNameServer.add(model.Task(TaskType.READ, null, cmds(1), 0, fileLength))
      } else if (cmds.length == 4) {
        MiniDFS.queueForNameServer.add(model.Task(TaskType.READ, null, cmds(1), cmds(2).toLong, cmds(3).toLong))
      } else {
        println(MiniDFS.CurrentTime() + " ERROR: parameter num is not right")
      }
    }
  }

  def deleteDataServer(id: Int): Unit = {
    var reader: FileReader = null
    var br: BufferedReader = null
    var writer: FileWriter = null
    var bw: BufferedWriter = null

    val buffer: StringBuilder = new StringBuilder

    try {
      reader = new FileReader(Configuration.ORIGIN_FILELIST)
      br = new BufferedReader(reader)

      var str: String = br.readLine()
      while (str != null) {
        if (str.startsWith("#")) {
          println(s"before delete: $str")
          val temp = str.split(" ")
          buffer.append(temp(0) + " " + temp(1))
          temp.slice(2, 5).foreach(index => {
            if (index.equals(id.toString)) {
              buffer.append(" " + "*")
            } else {
              buffer.append(" " + index)
            }
          })
          println(s"after delete: ${buffer.takeRight(5)}")
        } else {
          buffer.append(str)
        }
        buffer.append("\n")
        str = br.readLine()
      }

      writer = new FileWriter(Configuration.ORIGIN_FILELIST)
      bw = new BufferedWriter(writer)
      bw.write(buffer.toString())

      val path = Configuration.DATASERVER_PREFIX + id.toString + fileSeperator
      if (deleteNodeDir(path)) {
        println(s"Node $id has been down")
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      try {
        if (bw != null) {
          bw.close()
        }
        if (writer != null) {
          writer.close()
        }
        if (br != null) {
          br.close()
        }
        if (reader != null) {
          reader.close()
        }
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  def deleteNodeDir(path: String): Boolean = {
    val file = new File(path)
    if (file.isDirectory) {
      System.gc()
      val children = file.list()
      var filename: File = null
      for (i <- 0 until children.length) {
        filename = new File(path, children(i))
        val success = filename.delete()
        if (!success) {
          false
        }
      }
    }
    file.delete()
  }

  def recovery(): Unit = {
    var reader: FileReader = null
    var br: BufferedReader = null
    var writer: FileWriter = null
    var bw: BufferedWriter = null

    val buffer: StringBuilder = new StringBuilder

    var num = detectDataServer()
    while (num != -1) {
      println(s"Node $num has been down")
      var fileName: String = ""
      var block = 0
      try {
        reader = new FileReader(Configuration.ORIGIN_FILELIST)
        br = new BufferedReader(reader)
        var str: String = br.readLine()
        while (str != null) {
          if (str.startsWith("#")) {
            println("before recovery: " + str)
            if (str.contains("*")) {
              str = doRecovery(str, num, block.toString + "_" + fileName)
            }
            block += 1
            println("after recovery: " + str)
          } else {
            fileName = str.split(" ")(0)
            block = 0
          }
          buffer.append(str + "\n")
          str = br.readLine()
        }

        writer = new FileWriter((Configuration.ORIGIN_FILELIST))
        bw = new BufferedWriter(writer)
        bw.write(buffer.toString())
        println(MiniDFS.CurrentTime() + s" INFO: File of Node $num has been recovered")
        num = detectDataServer()
      } catch {
        case e: IOException => e.printStackTrace()
      } finally {
        try {
          if (bw != null) {
            bw.close()
          }
          if (writer != null) {
            writer.close()
          }
          if (br != null) {
            br.close()
          }
          if (reader != null) {
            reader.close()
          }
        } catch {
          case e: IOException => e.printStackTrace()
        }
      }
    }
    println(MiniDFS.CurrentTime() + s" INFO: File of all dead node has been recovered")
  }

  def doRecovery(str: String, num: Int, fileName: String): String = {
    val invalidNode = str.split(" ").slice(2, 5)
    val remainNode = ArrayBuffer[Int]()
    var srcNode = "-1"
    for (i <- 0 until Configuration.DATASERVERNUM) {
      if (i != num && !invalidNode.contains(i.toString)){
        remainNode.append(i)
      }
    }
    invalidNode.foreach(id => if(!id.equals("*")) srcNode = id)
    // 恢复到原结点还是恢复到其他结点
//    val destNode = remainNode(0).toString
    val destNode = num.toString
    val srcFileName = Configuration.DATASERVER_PREFIX + srcNode + fileSeperator + fileName
    val destFileName = Configuration.DATASERVER_PREFIX + destNode + fileSeperator + fileName

    if (copyFile(srcFileName, destFileName, true)) {
      println(MiniDFS.CurrentTime() + s" INFO: $destFileName recovery successfully $srcFileName -> $destFileName")
    } else {
      println(MiniDFS.CurrentTime() + s" ERROR: $destFileName recovery failed")
    }
    str.replace("*", destNode)
  }

  def copyFile(srcFileName: String, destFileName: String, overlay: Boolean): Boolean = {
    val srcFile = new File(srcFileName)
    val destFile = new File(destFileName)

    if (!srcFile.exists()) {
      println(MiniDFS.CurrentTime() + s" ERROR: $srcFileName doesn't exist")
      return false
    } else if (!srcFile.isFile) {
      println(MiniDFS.CurrentTime() + s" ERROR: $srcFileName is not a File")
      return false
    }
    if (destFile.exists()) {
      if (overlay) {
        destFile.delete()
      }
    }


    var is: InputStream = null
    var bis: BufferedInputStream = null
    var os: OutputStream = null
    var bos: BufferedOutputStream = null

    try {
      is = new FileInputStream(srcFile)
      bis = new BufferedInputStream(is)
      os = new FileOutputStream(destFile)
      bos = new BufferedOutputStream(os)
      val buffer = new Array[Byte](1024)
      var byteRead = bis.read(buffer)
      while (byteRead != -1) {
        bos.write(buffer, 0, byteRead)
        byteRead = bis.read(buffer)
      }
      return true
    } catch {
      case e: FileNotFoundException => {
        e.printStackTrace()
        return false
      }
      case e: IOException => {
        e.printStackTrace()
        return false
      }
    } finally {
      try {
        if (bos != null) {
          bos.close()
        }
        if (os != null) {
          os.close()
        }
        if (bis != null) {
          bis.close()
        }
        if (is != null) {
          is.close()
        }
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }

  }

  def detectDataServer(): Int = {
    var num = -1
    var dir: File = null
    for (i <- 0 until Configuration.DATASERVERNUM) {
      dir = new File(Configuration.DATASERVER_PREFIX + i.toString)
      if (!dir.isDirectory) {
        dir.mkdir()
        num = i
        return num
      }
    }
    num
  }


  def showFileStorage(): Unit = {
    var reader: FileReader = null
    var br:BufferedReader = null

    try {
      reader = new FileReader(Configuration.ORIGIN_FILELIST)
      br = new BufferedReader(reader)
      var str: String = br.readLine()
      while (str != null) {
        println(str)
        str = br.readLine()
      }
    } catch {
      case e: IOException => e.printStackTrace()
    }
  }

  def exit(): Unit = {
    isExit = true
    return
  }

  def isInteger(str: String): Boolean = {
    pattern.matcher(str).matches()
  }

  def getUploadFile(): File = {
    val fileChooser = new JFileChooser()
    fileChooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES)
    val returnVal = fileChooser.showOpenDialog(fileChooser)
    if (returnVal == JFileChooser.APPROVE_OPTION) {
      val filePath = fileChooser.getSelectedFile.getAbsolutePath()
      new File(filePath)
    } else {
      null
    }
  }

  def conflict(file: File): Boolean = {
    var reader: FileReader = null
    var br: BufferedReader = null
    try {
      reader = new FileReader(Configuration.ORIGIN_FILELIST)
      br = new BufferedReader(reader)
      var str: String = br.readLine()

      while (str != null) {
        val s = str.split(" ")
        if (s(0).equals(file.getName)) {
          return true
        }
        val skip = s(3).toInt
        for (i <- 0 until skip){
          br.readLine()
        }
        str = br.readLine()
      }
    } catch {
      case e: IOException => {
        e.printStackTrace()
      }
    } finally {
        try {
          if (br != null) {
            br.close()
          }
          if (reader != null) {
            reader.close()
          }
        } catch {
          case e: IOException => e.printStackTrace()
        }

    }
    false
  }

  def getLength(file: File): Long = {
    var reader: FileReader = null
    var br: BufferedReader = null
    try {
      reader = new FileReader(Configuration.ORIGIN_FILELIST)
      br = new BufferedReader(reader)
      var str:String = br.readLine()
      while (str != null) {
        if (!str.startsWith("#")) {
          val info = str.split(" ")
          if (info(0).equals(file.getName)) {
            return info(1).toLong
          }
        }
        str = br.readLine()
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
        try{
          if (br != null) {
            br.close()
          }
          if (reader != null) {
            reader.close()
          }
        } catch {
          case e: IOException => e.printStackTrace()
        }

    }
    0L
  }

}
