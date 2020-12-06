import java.nio.file.Path
import java.util.Date

class SnapshotFileTest extends TestWithCleanUp {
  test("SnapshotFile read write") {
    val fileObject = FileObject(Path.of("a/b/c"), System.currentTimeMillis())
    val snapshot = Snapshot(new Date(), Set(fileObject))
    val snapshotFile = new SnapshotFile(snapshot)
    val snapshotFilePath = snapshotFile.saveTo(Path.of("/tmp"))
    addToCleanUp(snapshotFilePath)
    assert(SnapshotFile.read(snapshotFilePath) == snapshot)
  }
}
