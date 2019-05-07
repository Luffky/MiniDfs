package sjtu.project

import sjtu.project.TaskType.TaskType
import java.io.File

case class Task(taskType: TaskType, file: File, fileName: String, offset: Long, length: Long, blockIndex: Long = 0, totalBlock: Long = 0, readUid: String = "") {

}
