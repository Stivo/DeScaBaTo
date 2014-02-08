//package ch.descabato
//
//import org.scalatest._
//import org.scalatest.Matchers._
//
//class BackupSpec extends FlatSpec {
//  import org.scalacheck.Gen._
//  
//  val toTest = new BackupIndexHandler() {
//    val config = new BackupFolderConfiguration()
//  }
//  
//  "detect roots" should "work" in {
//    testWith(List("a/b"), "a/b/c2", "a/b/c1")
//    testWith(List("a/b/"), "a/b/c2", "a/b/c1")
//    testWith(List("a/b/c2", "a/b/c1"))
//    testWith(List("/a/b/c2", "/a/b/c1"))
//    testWith(List("/"), "/a/b/c1", "/a", "/b")
//    testWith(List("a1", "a2"), "a1/b", "a2/c/d/e", "a1/c", "a2/b")
//    intercept[IllegalStateException] {
//      testWith(List("a1", "a2"), "a1/b", "a2/c/d/e", "a1/c", "a2/b", "a2/b")
//    }
//  }
//  
//  def testWith(expected: List[String], candidates: String *) {
//    val l = candidates.toList
//    val folders = (l ++ expected).map(x => new FolderDescription(x.toString, null))
//    val expectedFolders = folders.drop(candidates.size)
//    for (perm <- folders.permutations) {
//      toTest.detectRoots(perm) should contain only (expectedFolders: _*)
//    }
//  }
//  
//}
