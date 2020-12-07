import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Path

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestWithCleanUp extends FunSuite with BeforeAndAfter {
  private var toCleanUp = List[Either[Path, File]]()

  protected def createTempFile(filename: String, content: String): Path = {
    val file = Path.of(s"/tmp/$filename")
    addToCleanUp(file)
    val writer = new BufferedWriter(new FileWriter(file.toFile))
    writer.write(content)
    writer.close()
    file
  }

  protected def createFileObject(filename: String, content: String, lastModifiedTime: Long): (Path, FileObject) = {
    val file = createTempFile(filename, content)
    val base = file.getParent
    val filenamePath = file.getFileName
    (base, FileObject(filenamePath, lastModifiedTime))
  }

  protected def createFileObject(filename: String, content: String): (Path, FileObject) = {
    createFileObject(filename, content, System.currentTimeMillis())
  }

  protected def addToCleanUp(path: Path): Unit = {
    toCleanUp = Left(path) :: toCleanUp
  }

  protected def addToCleanUp(file: File): Unit = {
    toCleanUp = Right(file) :: toCleanUp
  }

  private def delete(f: File): Unit = {
    if (f.isDirectory) {
      FileUtils.deleteDirectory(f)
    } else {
      f.delete()
    }
  }

  after {
    toCleanUp.map({
      case Left(path) => path.toFile
      case Right(file) => file
    }).foreach(delete)
  }
}
