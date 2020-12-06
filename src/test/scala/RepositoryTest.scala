import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Path
import scala.io.Source

class RepositoryTest extends TestWithCleanUp {
  def createRepo(): Repository = {
    val repositoryPath = Path.of("/tmp/testRepo")
    addToCleanUp(repositoryPath)
    repositoryPath.toFile.mkdir()
    new Repository(repositoryPath)
  }

  def createFile(filename: String, content: String): (Path, Path) = {
    val file = Path.of(s"/tmp/$filename")
    addToCleanUp(file)
    val writer = new BufferedWriter(new FileWriter(file.toFile))
    writer.write(content)
    writer.close()
    (file.getParent, file.getFileName)
  }

  def createFileObject(filename: String, content: String, lastModifiedTime: Long): (Path, FileObject) = {
    val (base, filePath) = createFile(filename, content)
    (base, FileObject(filePath, lastModifiedTime))
  }

  def createFileObject(filename: String, content: String): (Path, FileObject) = {
    createFileObject(filename, content, System.currentTimeMillis())
  }

  test("Repository.save") {
    val content = "1234567890"
    val (base, fileObject) = createFileObject("test", content)
    val repository = createRepo()
    repository.save(base, fileObject)
    val blobSource = Source.fromFile(fileObject.serializeToPath(repository.blobFolder).toFile)
    val blobContent = blobSource.mkString
    blobSource.close()
    assert(blobContent == content)
  }

  test("Repository.delete") {
    val repo = createRepo()
    val (base, fileObject1) = createFileObject("test1", "", 0)
    val (_, fileObject2) = createFileObject("test2", "", 0)
    val (_, fileObject3) = createFileObject("test3", "", 1)
    repo.save(base, fileObject1)
    repo.save(base, fileObject2)
    repo.save(base, fileObject3)
    def blobFolderSubFolderCount = repo.blobFolder.toFile.listFiles.length
    assert(blobFolderSubFolderCount == 2)
    repo.delete(fileObject1)
    assert(blobFolderSubFolderCount == 2)
    repo.delete(fileObject3)
    assert(blobFolderSubFolderCount == 1)
    repo.delete(fileObject2)
    assert(blobFolderSubFolderCount == 0)
  }
}
