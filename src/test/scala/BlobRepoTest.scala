import java.nio.file.Path

import scala.io.Source

class BlobRepoTest extends TestWithCleanUp {
  test("BlobRepo put get delete") {
    val blobRepoPath = Path.of("/tmp/BlobRepoTest")
    blobRepoPath.toFile.mkdir()
    addToCleanUp(blobRepoPath)
    val blobRepo = new BlobRepo(blobRepoPath)
    val content = "123456"
    val (base, fileObject) = createFileObject("testBlobRepo", content)
    blobRepo.put(base, fileObject)
    val fileInRepo = blobRepo.get(fileObject)
    assert(fileInRepo.nonEmpty)
    val fileSource = Source.fromFile(fileInRepo.get.toFile)
    assert(fileSource.mkString == content)
    blobRepo.delete(fileObject)
    assert(blobRepo.get(fileObject).isEmpty)
    assert(!fileInRepo.get.toFile.isFile)
  }
}
