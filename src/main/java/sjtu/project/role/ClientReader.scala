package sjtu.project.role

import sjtu.project.MiniDFS

import scala.collection.mutable


class ClientReader(isPrint: Boolean) extends Runnable{
  val resultBuffer = new mutable.HashMap[String, Array[Array[Byte]]]()
  val blockMap = new mutable.HashMap[String, Long]()
  override def run(): Unit = {
    while (true) {
      val result = MiniDFS.queueForReadData.take()
      val task = result._1
      println(MiniDFS.CurrentTime() + s" INFO: read information: $result")
      if (isPrint) {
        var remain: Int = 0
        if (resultBuffer.contains(result._1.readUid)) {
          remain = blockMap.get(task.readUid).get.toInt - 1
          resultBuffer.get(task.readUid).get(task.blockIndex.toInt) = result._3
          blockMap.put(task.readUid, remain)
        } else {
          remain = task.totalBlock.toInt - 1
          resultBuffer.put(task.readUid, new Array[Array[Byte]](task.totalBlock.toInt))
          blockMap.put(task.readUid, remain)
          resultBuffer.get(task.readUid).get(task.blockIndex.toInt) = result._3
        }

        if (remain == 0) {
          val str = new String(resultBuffer.get(task.readUid).get.flatten)
          println(str)
          resultBuffer.remove(task.readUid)
          blockMap.remove(task.readUid)
        }
      }
    }
  }
}

