import java.nio.file.Path

import scala.io.Source

class BlobRepoTest extends TestWithCleanUp {
  private def createBlobRepo(path: Path = Path.of("/tmp/BlobRepoTest")): BlobRepo = {
    val blobRepoPath = path
    blobRepoPath.toFile.mkdir()
    addToCleanUp(blobRepoPath)
    new BlobRepo(blobRepoPath)
  }

  test("BlobRepo put get delete") {
    val blobRepo = createBlobRepo()
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

  test("BlobRepo.delete") {
    val blobRepoPath = Path.of("/tmp/BlobRepoTest")
    val repo = createBlobRepo(blobRepoPath)
    val (base, fileObject1) = createFileObject("test1", "", 0)
    val (_, fileObject2) = createFileObject("test2", "", 0)
    val (_, fileObject3) = createFileObject("test3", "", 1)
    repo.put(base, fileObject1)
    repo.put(base, fileObject2)
    repo.put(base, fileObject3)

    def blobFolderSubFolderCount = blobRepoPath.toFile.listFiles.length

    assert(blobFolderSubFolderCount == 2)
    repo.delete(fileObject1)
    assert(blobFolderSubFolderCount == 2)
    repo.delete(fileObject3)
    assert(blobFolderSubFolderCount == 1)
    repo.delete(fileObject2)
    assert(blobFolderSubFolderCount == 0)
  }
}
