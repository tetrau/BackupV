import java.io.{BufferedWriter, FileWriter}
import java.nio.ByteBuffer
import java.nio.file.{Path, Paths}
import java.text.{ParsePosition, SimpleDateFormat}
import java.util.{Base64, Date, TimeZone}

import scala.io.Source
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

object SnapshotFile {
  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  dateFormat.setTimeZone(TimeZone.getDefault)

  private def parseDate(dateString: String): Date = {
    dateFormat.parse(dateString, new ParsePosition(0))
  }

  def read(filename: String, content: String): Snapshot = {
    val dateString = filename.split('.')(1)
    val timestamp = parseDate(dateString)
    val fileObjects = content.split('\n')
      .map(_.trim).map(Path.of(_))
      .map(FileObject.deserializeFromPath)
    Snapshot(timestamp, fileObjects.toSet)
  }

  def read(path: Path): Snapshot = {
    val filename = path.getFileName.toString
    val fileSource = Source.fromFile(path.toFile)
    val fileContent = fileSource.mkString
    fileSource.close
    read(filename, fileContent)
  }
}

class SnapshotFile(val snapshot: Snapshot) {
  private def dateToString(date: Date): String = {
    SnapshotFile.dateFormat.format(date)
  }

  val filename: String = {
    s"snapshot.${dateToString(snapshot.timestamp)}.txt"
  }

  lazy val content: String = {
    snapshot.fileObjects.map(_.serializeToPath()).mkString("\n")
  }

  def saveTo(folder: Path): Path = {
    val path = folder.resolve(filename)
    val file = path.toFile
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(content)
    writer.close()
    path
  }
}