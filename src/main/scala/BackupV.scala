import java.nio.ByteBuffer
import java.nio.file.{Paths, Path}
import java.util.{Base64, Date}

import scala.jdk.CollectionConverters._

object FileObject {
  private def deserialize(s: String): FileObject = {
    val serializedBytes = Base64.getUrlDecoder.decode(s)
    val lastModifiedTime = ByteBuffer.wrap(serializedBytes.take(8).reverse).getLong()
    val filePathAsString = new String(serializedBytes.drop(8), "UTF-8")
    val filePath = Paths.get(filePathAsString)
    FileObject(filePath, lastModifiedTime)
  }

  def deserializeFromPath(path: Path): FileObject = {
    deserialize(path.iterator().asScala.mkString(""))
  }

  private def relativePath(base: Path, absPath: Path): Path = {
    require(base.isAbsolute && absPath.isAbsolute)
    base.relativize(absPath)
  }

  def apply(base: Path, absPath: Path, lastModifiedTime: Long): FileObject = {
    FileObject(relativePath(base, absPath), lastModifiedTime)
  }
}

case class FileObject(filePath: Path, lastModifiedTime: Long) {
  private def serialize(): String = {
    val pathAsBytes: Array[Byte] = filePath.toString.getBytes("UTF-8")
    val buffer = ByteBuffer.allocate(8 + pathAsBytes.length)
    buffer.put(pathAsBytes.reverse)
    buffer.putLong(lastModifiedTime)
    Base64.getUrlEncoder.encodeToString(buffer.array.reverse)
  }

  def serializeToPath(): Path = {
    val serializedResult = serialize()
    Paths.get(serializedResult.take(2), serializedResult.drop(2).sliding(255, 255).toSeq: _*)
  }
}

case class Snapshot(timestamp: Date, fileObjects: Set[FileObject]) {
  def addFileObject(fileObject: FileObject): Snapshot = {
    Snapshot(timestamp, fileObjects + fileObject)
  }

  def removeFileObject(fileObject: FileObject): Snapshot = {
    Snapshot(timestamp, fileObjects - fileObject)
  }

  def replaceFileObject(oldFileObject: FileObject, newFileObject: FileObject): Snapshot = {
    this.removeFileObject(oldFileObject).addFileObject(newFileObject)
  }
}