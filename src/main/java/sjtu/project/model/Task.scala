package sjtu.project.model

import java.io.File

import sjtu.project.model.TaskType.TaskType

case class Task(taskType: TaskType, file: File, fileName: String, offset: Long, length: Long, blockIndex: Long = 0, totalBlock: Long = 0, readUid: String = "") {

}
