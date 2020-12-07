import java.nio.file.Path
import scala.io.Source

class RepositoryTest extends TestWithCleanUp {
  def createRepo(): Repository = {
    val repositoryPath = Path.of("/tmp/testRepo")
    addToCleanUp(repositoryPath)
    repositoryPath.toFile.mkdir()
    new Repository(repositoryPath)
  }

  test("Repository.save") {
    val content = "1234567890"
    val (base, fileObject) = createFileObject("test", content)
    val repository = createRepo()
    repository.put(base, fileObject)
    val blobSource = Source.fromFile(repository.get(fileObject).get.toFile)
    val blobContent = blobSource.mkString
    blobSource.close()
    assert(blobContent == content)
  }
}
