import java.nio.file.Path

class DirectoryScanTest extends TestWithCleanUp {
  test("DirectoryScan.scan") {
    val file1 = createTempFile(Path.of("DirectoryScanTest/1"), "")
    val file2 = createTempFile(Path.of("DirectoryScanTest/test/2"), "")
    val dir3 = Path.of("/tmp/DirectoryScanTest/test2")
    addToCleanUp(dir3)
    dir3.toFile.mkdir()
    val allFiles = Set.from(DirectoryScan.scan(Path.of("/tmp/DirectoryScanTest")))
    assert(allFiles.contains(file1))
    assert(allFiles.contains(file2))
    assert(!allFiles.contains(dir3))
  }
}
