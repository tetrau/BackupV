import java.io.{BufferedWriter, FileWriter}
import java.nio.file.Path
import scala.io.Source

class RepositoryTest extends TestWithCleanUp {
  test("Repository.save") {
    val file = Path.of("/tmp/test")
    addToCleanUp(file)
    val repositoryPath = Path.of("/tmp/testRepo")
    addToCleanUp(repositoryPath)
    repositoryPath.toFile.mkdir()
    val writer = new BufferedWriter(new FileWriter(file.toFile))
    val content = "test1234"
    writer.write("test1234")
    writer.close()
    val fileObject = FileObject(file.getFileName, 0)
    val repository = new Repository(repositoryPath)
    repository.save(file.getParent,fileObject)
    val blobSource = Source.fromFile(fileObject.serializeToPath(repository.blobFolder).toFile)
    val blobContent = blobSource.mkString
    blobSource.close()
    assert(blobContent == content)
  }
}
