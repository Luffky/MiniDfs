package sjtu.project.role

import java.io._
import java.util.concurrent.LinkedBlockingQueue

import sjtu.project.model.{Configuration, Task, TaskType}
import sjtu.project.MiniDFS

class DataServer(private val index: Int) extends Runnable {
  var task: Task = null
  println(MiniDFS.CurrentTime() + " INFO: DataServer-" + index.toString + " start" )
  override def run(): Unit = {
    while (true) {
      try {
        val field = MiniDFS.getClass.getDeclaredField(s"queueForDataServer$index")
        field.setAccessible(true)
        task = field.get(MiniDFS)
          .asInstanceOf[LinkedBlockingQueue[Task]]
          .take()
      } catch {
        case e: InterruptedException => e.printStackTrace()
      }

      if (TaskType.WRITE == task.taskType) {
        writeFile(task.file, task.offset, task.fileName)

      } else {
        val result = readFile(task.fileName, task.offset, task.length)
        if (result != null) {
          MiniDFS.queueForReadData.put(Tuple3(task, index, result))
        } else {
          println(MiniDFS.CurrentTime() + " ERROR: Read data unsuccessfully")
        }
      }
    }
  }

  def readFile(dataBlockName: String, offset: Long, length: Long): Array[Byte] = {
    var raf: RandomAccessFile = null
    try {
      val readAddr = Configuration.DATASERVER_DIRS(index) + dataBlockName
      raf = new RandomAccessFile(readAddr, "r")
      val buffer = new Array[Byte](length.toInt)
      raf.seek(offset)
      raf.read(buffer, 0, length.toInt)
      return buffer
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      try {
        if (raf != null) {
          raf.close()
        }
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
    null
  }

  def writeFile(f: File, offset: Long, dataBlockName: String): Unit = {
    var raf: RandomAccessFile = null
    var os: OutputStream = null
    try {
      raf = new RandomAccessFile(f, "r")
      val buffer = new Array[Byte](Configuration.LENGTHOFBLOCK.toInt)
      raf.seek(offset)
      val length = raf.read(buffer)
      val writeAddr = Configuration.DATASERVER_DIRS(index) + dataBlockName
      os = new FileOutputStream(new File(writeAddr))
      os.write(buffer, 0, length)
      os.flush()
      println(MiniDFS.CurrentTime() + s" INFO: Write complete (${f.getName}: $dataBlockName)")
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      try {
        if (os != null) {
          os.close()
        }
        if (raf != null) {
          raf.close()
        }
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }
}
