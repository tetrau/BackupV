import java.nio.file.Path
import java.util.Date

class SnapshotRepoTest extends TestWithCleanUp {
  test("SnapshotRepo put get delete and list") {
    val fileObject = FileObject(Path.of("/a/b/c"), 0)
    val snapshot1 = Snapshot(new Date(0), Set(fileObject))
    val snapshot2 = Snapshot(new Date(1), Set(fileObject))
    val snapshotRepoPath = Path.of("/tmp/SnapshotRepoTest")
    snapshotRepoPath.toFile.mkdir()
    addToCleanUp(snapshotRepoPath)
    val snapshotRepo = new SnapshotRepo(snapshotRepoPath)
    assert(snapshotRepo.listTimestamps().isEmpty)
    snapshotRepo.put(snapshot1)
    assert(snapshotRepo.listTimestamps().length == 1)
    snapshotRepo.put(snapshot2)
    assert(snapshotRepo.listTimestamps().toSet == Set(snapshot1.timestamp, snapshot2.timestamp))
    assert(snapshotRepo.get(snapshot1.timestamp).contains(snapshot1))
    snapshotRepo.delete(snapshot1)
    assert(snapshotRepo.get(snapshot1.timestamp).isEmpty)
    assert(snapshotRepo.listTimestamps().length == 1)
    snapshotRepo.delete(snapshot2)
    assert(snapshotRepo.listTimestamps().isEmpty)
  }
}
