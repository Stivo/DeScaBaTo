package ch.descabato.rocks

import java.io.File

import ch.descabato.core.model.BackupPart
import ch.descabato.core.model.FolderDescription
import ch.descabato.frontend.RestoreConf
import ch.descabato.utils._

import scala.collection.mutable

class RestoredPathLogic(val folders: Seq[FolderDescription], val restoreConf: RestoreConf) {

  sealed trait Result

  case object IsSubFolder extends Result

  case object IsTopFolder extends Result

  case object IsUnrelated extends Result

  case object IsSame extends Result

  private def isRelated(folder: BackupPart, relatedTo: BackupPart): Result = {
    var folderParts = folder.pathParts.toList
    var relatedParts = relatedTo.pathParts.toList
    while (relatedParts.nonEmpty && relatedParts.headOption == folderParts.headOption) {
      relatedParts = relatedParts.tail
      folderParts = folderParts.tail
    }
    if (folderParts.lengthCompare(folder.pathParts.size) == 0) {
      IsUnrelated
    } else {
      (folderParts, relatedParts) match {
        case (Nil, x :: _) => IsSubFolder
        case (x :: _, Nil) => IsTopFolder
        case (x :: _, y :: _) => IsUnrelated
        case _ => IsSame
      }
    }
  }

  private def detectRoots(folders: Iterable[BackupPart]): Seq[FolderDescription] = {
    var candidates = mutable.Buffer[BackupPart]()
    folders.view.filter(_.isFolder).foreach {
      folder =>
        if (folder.path == "/") return List(folder.asInstanceOf[FolderDescription])
        // compare with each candidate
        var foundACandidate = false
        candidates = candidates.map {
          candidate =>
            // case1: This folder is a subfolder of candidate
            //		=> nothing changes, but search is aborted
            // case2: this folder is a parent of candidate
            //		=> this folder should replace that candidate, search is aborted
            // case3: this folder is not related to any candidate
            //		=> folder is added to candidates
            isRelated(candidate, folder) match {
              case IsUnrelated => candidate
              case IsTopFolder =>
                foundACandidate = true; folder
              case IsSubFolder =>
                foundACandidate = true; candidate
              case IsSame => throw new IllegalStateException("Duplicate entry found: " + folder + " " + candidate)
            }
        }.distinct
        if (!foundACandidate) {
          candidates += folder
        }
    }
    candidates.flatMap {
      case x: FolderDescription => List(x)
    }.toList
  }

  private val roots: Iterable[FolderDescription] = detectRoots(folders)
  private val relativeToRoot: Boolean = roots.size == 1 || roots.map(_.name).toList.distinct.lengthCompare(roots.size) == 0

  private def getRoot(sub: String): Option[FolderDescription] = {
    // TODO this asks for a refactoring
    val fd = FolderDescription(sub, null)
    roots.find {
      x => val r = isRelated(x, fd); r == IsSubFolder || r == IsSame
    }
  }

  def makePath(path: String, maybeOutside: Boolean = false): File = {
    if (restoreConf.restoreToOriginalPath()) {
      return new File(path)
    }

    def cleaned(s: String) = if (Utils.isWindows) s.replace(":", "_") else s

    val dest = new File(restoreConf.restoreToFolder())
    if (relativeToRoot) {
      val relativeTo = getRoot(path) match {
        case Some(x: BackupPart) => x.path
        // this is outside, so we return the path
        case None if !maybeOutside => throw new IllegalStateException("This should be inside the roots, some error")
        case None => return new File(path)
      }
      FileUtils.getRelativePath(dest, new File(relativeTo), path)
    } else {
      new File(dest, cleaned(path))
    }
  }


}
