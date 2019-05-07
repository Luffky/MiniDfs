package sjtu.project

import java.io._
import java.util.concurrent.{LinkedBlockingDeque, LinkedBlockingQueue}
import java.util.regex.Pattern
import Configuration.fileSeperator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class NameServer extends Runnable{
  println(MiniDFS.CurrentTime() + " INFO: NameServer start")
  // 线程
  override def run(): Unit = {
    while (true) {
      var task: Task = null
      try {
        task = MiniDFS.queueForNameServer.take()
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }

      try{
        if (TaskType.WRITE == task.taskType) {
          NameServer.writeFile(task.file)
        } else {
          NameServer.readFile(task.fileName, task.offset, task.length)
        }
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }
    }
  }
}

object NameServer {
  val dataInDataServer = new mutable.HashMap[String, ArrayBuffer[Int]]()
  val fileMd5sum = new mutable.HashMap[String, String]()
  val dataOfFile = new mutable.HashMap[String, ArrayBuffer[String]]()

  val numPattern = Pattern.compile("""\d+""")
  val random = new Random()

  var indexOfDataServer: Int = 0

  //启动NameServer前的预处理，读取元数据到内存中
  def apply(): NameServer = {
    val originFile = new File(Configuration.ORIGIN_FILELIST)
    if (originFile.exists()) {
      var reader: FileReader = null
      var br: BufferedReader = null
      try{
        reader = new FileReader(originFile)
        br = new BufferedReader(reader)
        var index: Int = 0
        val fileList = ArrayBuffer[String]()
        var fileName: String = null

        var str: String = br.readLine()
        while (str != null) {
          if (str.startsWith("#")) {
            fileList.append(index + "_" + fileName)

            val info = str.split(" ")
            val numList = new ArrayBuffer[Int](initialSize = 3)
            info.slice(2, 5).foreach(num => {
              num match {
                case v1 if numPattern.matcher(v1).matches() => numList.append(v1.toInt)
                case "*" => numList.append(-1)
                case _ => throw new NameServerException("metadata table has been corrupted")
              }
            })
            dataInDataServer.put(index + "_" + fileName, numList)
            index += 1
          }
          else {
            val info = str.split(" ")
            println(fileName)
            println(fileList)
            if (fileName != null) {
              dataOfFile.put(fileName, fileList.clone())
            }
            fileName = info(0)
            fileMd5sum.put(fileName, info(2))

            index = 0
            fileList.clear()
          }
          str = br.readLine()
        }
        dataOfFile.put(fileName, fileList)
      } catch {
        case e: IOException => e.printStackTrace()
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
    } else {
      originFile.createNewFile()
    }
    val dataFileRoot = new File(Configuration.DATASERVER_PREFIX)
    if (!dataFileRoot.exists()) {
      dataFileRoot.mkdir()
    }
    for (i <- Configuration.DATASERVER_DIRS) {
      val tempFile = new File(i)
      if (!tempFile.exists()) {
        tempFile.mkdir()
      }
    }
    new NameServer()
  }

  //将元数据记录到外部文件中，容错，供下次重启读取
  def record(filelength: Long, fileName: String, checkResult: String, count: Int): Unit = {
    var writer: FileWriter = null
    var bw: BufferedWriter = null
    try {
      writer = new FileWriter(Configuration.ORIGIN_FILELIST, true)
      bw = new BufferedWriter(writer)
      bw.write(s"$fileName $filelength $checkResult $count \n")
      for (i <- 0 until count) {
        val index = i + "_" + fileName
        bw.write(s"#$i: ${Math.min(Configuration.LENGTHOFBLOCK, filelength - i * Configuration.LENGTHOFBLOCK)} "
                + dataInDataServer.get(index).get(0) + " "
                + dataInDataServer.get(index).get(1) + " "
                + dataInDataServer.get(index).get(2) + "\n")
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
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  //写输入预操作
  def writeFile(f: File): Unit = {
    val checkResult = MD5Util.convertToHexString(MD5Util.md5sum(f))
    fileMd5sum.put(f.getName, checkResult)

    val fileList = ArrayBuffer[String]()
    val filelength = f.length()
    val count = Math.ceil(filelength.toFloat / Configuration.LENGTHOFBLOCK).toInt
    println(MiniDFS.CurrentTime() + s" INFO: write begin (source file length $filelength is divided into $count blocks)")

    for (i <- 0 until count) {
      val fileBlockName = i + "_" + f.getName
      fileList.append(fileBlockName)
      val numList = new ArrayBuffer[Int](3)
      for (k <- 0 until 3) {
        numList.append(indexOfDataServer)
        try {
          val field = MiniDFS.getClass.getDeclaredField(s"queueForDataServer$indexOfDataServer")
          field.setAccessible(true)
          field.get(MiniDFS)
            .asInstanceOf[LinkedBlockingQueue[Task]]
            .put(Task(TaskType.WRITE, f, fileBlockName, Configuration.LENGTHOFBLOCK * i, 0))
        } catch {
          case e: InterruptedException => e.printStackTrace()
          case e: NoSuchFieldException => e.printStackTrace()
          case e: SecurityException => e.printStackTrace()
        }
        indexOfDataServer = (indexOfDataServer + 1) % Configuration.DATASERVERNUM
      }
      dataInDataServer.put(fileBlockName, numList)
    }
    dataOfFile.put(f.getName, fileList)
    record(filelength, f.getName, checkResult, count)
  }

  //读取数据预操作，控制偏移量和文件，实际的读取操作由DataServer进行
//  @throws(classOf[InterruptedException])
  def readFile(fileName: String, offset: Long, length: Long): Unit = {
    var num = (offset / Configuration.LENGTHOFBLOCK).toInt
    var len = length
    // 以便组合读取的数据
    var blockIndex = 0
    val totalBlock = totalBlockCalculate(num, length, offset)
    val uid = System.currentTimeMillis().toString
    println(s"partition number: $num")
    if (length <= (num + 1) * Configuration.LENGTHOFBLOCK - offset) {
      readBlockFile(num + "_" + fileName, offset - num * Configuration.LENGTHOFBLOCK, len, blockIndex, totalBlock, uid)
    } else {
      readBlockFile(num + "_" + fileName, offset - num * Configuration.LENGTHOFBLOCK
        , (num + 1) * Configuration.LENGTHOFBLOCK - offset, blockIndex, totalBlock, uid)
      len -= (num + 1) * Configuration.LENGTHOFBLOCK - offset
      while (len > Configuration.LENGTHOFBLOCK) {
        num += 1
        blockIndex += 1
        readBlockFile(num + "_" + fileName, 0, Configuration.LENGTHOFBLOCK, blockIndex, totalBlock, uid)
        len -= Configuration.LENGTHOFBLOCK
      }
      if (len > 0) {
        num += 1
        blockIndex += 1
        readBlockFile(num + "_" + fileName, 0, len, blockIndex, totalBlock, uid)
      }
    }

    if (md5Check(fileName)) {
      println(MiniDFS.CurrentTime() + " INFO: read success")
    } else {
      println(MiniDFS.CurrentTime() + " INFO: read failed")
    }
  }

  // 向DataServer异步发送任务
  def readBlockFile(blockFileName: String, offset: Long, length: Long, blockIndex: Long, totalBlock: Long, uid: String): Unit = {
    var index = -1
    while (index == -1) {
      index = dataInDataServer.get(blockFileName).get(random.nextInt(3))
    }
    try {
      val filed = MiniDFS.getClass.getDeclaredField(s"queueForDataServer$index")
      filed.setAccessible(true)
      filed.get(MiniDFS)
        .asInstanceOf[LinkedBlockingQueue[Task]]
        .put(Task(TaskType.READ, null, blockFileName, offset, length, blockIndex, totalBlock, uid))
    } catch {
      case e: InterruptedException => e.printStackTrace()
      case e: NoSuchFieldException => e.printStackTrace()
      case e: SecurityException => e.printStackTrace()
    }
  }

  def totalBlockCalculate(num: Int, length: Long, offset: Long): Long = {
    var totalBlock = 1
    var len = length
    len -= (num + 1) * Configuration.LENGTHOFBLOCK - offset
    while (len > 0) {
      totalBlock += 1
      len -= Configuration.LENGTHOFBLOCK
    }
    totalBlock
  }

  def md5Check(fileName: String): Boolean = {
    val readAddr = Configuration.DATASERVER_PREFIX
    val fileList = new ArrayBuffer[File]()
    val fileSegmentList = dataOfFile.get(fileName)
    fileSegmentList.get.foreach(seg => {
      var index = -1
      while (index == -1) {
        index = dataInDataServer.get(seg).get(random.nextInt(3))
      }
      fileList.append(new File(s"$readAddr$index$fileSeperator$seg"))
    })
    if (MD5Util.convertToHexString(MD5Util.md5sum(fileList)).equals(fileMd5sum.get(fileName).get)) {
      println(MiniDFS.CurrentTime() + " INFO: MD5 check passed")
      true
    } else {
      println(MiniDFS.CurrentTime() + " INFO: MD5 check failed")
      false
    }
  }

}

class NameServerException(message: String) extends MiniDFSException(message) {

}