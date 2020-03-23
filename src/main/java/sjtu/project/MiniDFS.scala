package sjtu.project

import java.text.SimpleDateFormat
import java.util
import java.util.concurrent._
import java.util.{Collections, Date}

import sjtu.project.model.{Configuration, Task}
import sjtu.project.role.{ClientReader, DataServer, NameServer}

object MiniDFS {
  val queueForDataServer0 = new LinkedBlockingQueue[Task]()
  val queueForDataServer1 = new LinkedBlockingQueue[Task]()
  val queueForDataServer2 = new LinkedBlockingQueue[Task]()
  val queueForDataServer3 = new LinkedBlockingQueue[Task]()
  val queueForNameServer = new LinkedBlockingQueue[Task]()
  //负责接收读取的数据
  val queueForReadData =  new LinkedBlockingQueue[Tuple3[Task, Int, Array[Byte]]]()

  val dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")

  val threadPool= new ThreadPoolExecutor(0, Int.MaxValue, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable])

  def CurrentTime(): String = {
    dateFormat.format(new Date())
  }

  def help(): Unit = {
    println("Command Usage: ")
    println("--------------------")
    println("              1. write + choose 写文件(调用文件选择器)")
    println("              2. write + filePath 写文件(根据文件路径)")
    println("              3. read + filePath 读文件(根据文件路径)")
    println("              4. read + filePath + offset + length 读文件(根据文件路径 + 偏移项 + 长度)")
    println("              5. delete + DataServer' number 删除对应序号的数据节点")
    println("              6. recovery 恢复丢失的文件")
    println("              7. ls 查看当前文件的存储情况")
    println("              8. help 命令提示")
    println("              9. exit 退出")
  }

//  @throws(classOf[InterruptedException])
  def main(args: Array[String]): Unit = {
    try{
      println("Mini-DFS")
      println("********************")
      help()
      println("********************")

      threadPool.execute(new ClientReader(isPrint = true))

      val nameServer = NameServer()
      threadPool.execute(nameServer)
      println("********************")
      val dataServerList = Collections.synchronizedList(new util.ArrayList[DataServer]())
      for (i <- 0 until Configuration.DATASERVERNUM) {
        dataServerList.add(new DataServer(i))
        threadPool.execute(dataServerList.get(i))
      }


      CommandProcess.Process()

    } finally {
      if (threadPool != null && !threadPool.isShutdown) {
        threadPool.shutdown()
      }
      System.exit(0)
    }



  }
}
