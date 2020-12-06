import java.io.File
import java.nio.file.Path

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestWithCleanUp extends FunSuite with BeforeAndAfter {
  private var toCleanUp = List[Either[Path, File]]()

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
