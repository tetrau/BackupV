import java.nio.file.Path
import org.scalatest.FunSuite

class FileObjectTest extends FunSuite {
  test("FileObject serialize and deserialize") {
    val fileObject = FileObject(Path.of("aaaa/bbb/ccccc"), System.currentTimeMillis())
    assert(fileObject == FileObject.deserializeFromPath(fileObject.serializeToPath()))
  }

  test("FileObject.apply equivalent") {
    val time = System.currentTimeMillis()
    val fileObject1 = FileObject.apply(Path.of("b/c"), time)
    val fileObject2 = FileObject.apply(Path.of("/a"), Path.of("/a/b/c"), time)
    assert(fileObject1 == fileObject2)
  }

}
