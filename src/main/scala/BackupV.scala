import java.io.{BufferedWriter, FileWriter}
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}
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

  def serializeToPath(base: Path): Path = {
    base.resolve(serializeToPath())
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


class SnapshotRepo(path: Path) {
  private val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  dateFormat.setTimeZone(TimeZone.getDefault)

  private def parseDate(dateString: String): Date = {
    dateFormat.parse(dateString, new ParsePosition(0))
  }

  private def dateToString(date: Date): String = {
    dateFormat.format(date)
  }

  private def dateToFilename(date: Date): String = {
    s"snapshot.${dateToString(date)}.txt"
  }

  private def content(snapshot: Snapshot): String = {
    snapshot.fileObjects.map(_.serializeToPath()).mkString("\n")
  }

  private def getSnapshotFilePath(timestamp: Date): Path = {
    path.resolve(dateToFilename(timestamp))
  }

  private def getSnapshotFilePath(snapshot: Snapshot): Path = {
    getSnapshotFilePath(snapshot.timestamp)
  }

  def put(snapshot: Snapshot): Path = {
    val snapshotFilePath = getSnapshotFilePath(snapshot)
    val file = snapshotFilePath.toFile
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(content(snapshot))
    writer.close()
    path
  }

  def get(timestamp: Date): Option[Snapshot] = {
    val snapshotFilePath = getSnapshotFilePath(timestamp)
    if (snapshotFilePath.toFile.isFile) {
      val fileSource = Source.fromFile(snapshotFilePath.toFile)
      val fileObjects = fileSource.mkString.split('\n')
        .map(_.trim).map(Path.of(_))
        .map(FileObject.deserializeFromPath)
      Some(Snapshot(timestamp, fileObjects.toSet))
    } else {
      None
    }
  }

  def delete(snapshot: Snapshot): Unit = {
    val snapshotFilePath = getSnapshotFilePath(snapshot)
    val snapshotFile = snapshotFilePath.toFile
    if (snapshotFile.isFile) {
      snapshotFile.delete()
    }
  }

  def listTimestamps(): Seq[Date] = {
    path.toFile.listFiles
      .filter(_.isFile)
      .map(_.getName)
      .filter("snapshot\\.\\d{17}\\.txt".r.matches(_))
      .map(_.split('.')(1))
      .map(parseDate)
  }
}

class BlobRepo(path: Path) {
  def filePathInRepository(fileObject: FileObject): Path = {
    fileObject.serializeToPath(path)
  }

  def put(fileObjectBase: Path, fileObject: FileObject): Unit = {
    val filePathInRepo = filePathInRepository(fileObject)
    filePathInRepo.getParent.toFile.mkdirs()
    Files.copy(
      fileObjectBase.resolve(fileObject.filePath),
      filePathInRepo,
      java.nio.file.StandardCopyOption.REPLACE_EXISTING)
  }

  def get(fileObject: FileObject): Option[Path] = {
    val filePath = filePathInRepository(fileObject)
    if (filePath.toFile.isFile) {
      Some(filePath)
    } else {
      None
    }
  }

  def delete(fileObject: FileObject): Unit = {
    @scala.annotation.tailrec
    def deleteHelper(deletePath: Path): Unit = {
      val file = deletePath.toFile
      if (deletePath == path) {}
      else if (file.isFile || (file.isDirectory && file.listFiles().length == 0)) {
        file.delete()
        deleteHelper(deletePath.getParent)
      } else if (!file.isFile && !file.isDirectory) {
        throw new RuntimeException(s"Can not delete $fileObject from repository because $deletePath not exists")
      }
    }

    val filePathInRepo = filePathInRepository(fileObject)
    deleteHelper(filePathInRepo)
  }

}

class Repository(path: Path) {
  require(path.toFile.isDirectory, f"Repository $path must be a directory")
  private val snapshotFolder: Path = path.resolve("snapshot")
  private val blobFolder: Path = path.resolve("blob")

  private def createFolder(p: Path): Unit = {
    val file = p.toFile
    require(!file.isFile, f"$p already exists as a file")
    if (!file.exists()) {
      file.mkdir()
    }
  }

  createFolder(snapshotFolder)
  createFolder(blobFolder)
  private val blobRepo = new BlobRepo(blobFolder)
  private val snapshotRepo = new SnapshotRepo(snapshotFolder)

  def save(fileObjectBase: Path, fileObject: FileObject): Unit = {
    blobRepo.put(fileObjectBase, fileObject)
  }

  def save(snapshot: Snapshot): Unit = {
    snapshotRepo.put(snapshot)
  }

  def get(fileObject: FileObject): Option[Path] = {
    blobRepo.get(fileObject)
  }

  def delete(fileObject: FileObject): Unit = {
    blobRepo.delete(fileObject)
  }

  def delete(snapshot: Snapshot): Unit = {
    snapshotRepo.delete(snapshot)
  }
}