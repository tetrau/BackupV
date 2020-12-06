import java.nio.file.Path
import java.util.Date

import org.scalatest.FunSuite

class SnapshotFileTest extends FunSuite {
  test("SnapshotFile read write") {
    val fileObject = FileObject(Path.of("a/b/c"), System.currentTimeMillis())
    val snapshot = Snapshot(new Date(), Set(fileObject))
    val snapshotFile = new SnapshotFile(snapshot)
    val snapshotFilePath = snapshotFile.saveTo(Path.of("/tmp"))
    assert(SnapshotFile.read(snapshotFilePath) == snapshot)
  }
}
