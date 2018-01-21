package ch.descabato.core.commands

import java.io.{File, FileOutputStream}
import java.util.Date

import akka.stream.scaladsl.Source
import ch.descabato.core.Universe
import ch.descabato.core.actors.MetadataActor.BackupMetaData
import ch.descabato.core.model.FileMetadata
import ch.descabato.core_old.{BackupPart, FileAttributes, FolderDescription}
import ch.descabato.frontend.RestoreConf
import ch.descabato.utils.Implicits._
import ch.descabato.utils._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

class DoRestore(_universe: Universe) extends DoReadAbstract(_universe) with Utils {

  import universe.materializer

  def restoreFromDate(options: RestoreConf, date: Date): Unit = {
    restore(options, Some(date))
  }

  def restore(options: RestoreConf, date: Option[Date] = None): Unit = {
    val startup = Await.result(universe.startup(), 1.minute)
    val metadata = Await.result(universe.backupFileActor.retrieveBackup(date), 1.minute)
    if (startup) {
      restoreImpl(options, metadata)
    }
    logger.info("Finished restoring files")
  }

  private def restoreImpl(options: RestoreConf, metadata: BackupMetaData) = {
    implicit val restoreConf: RestoreConf = options
    val logic = new RestoredPathLogic(metadata.folders, options)
    restoreFolders(logic, metadata.folders)
    restoreFiles(logic, metadata.files)
    restoreFolders(logic, metadata.folders)
  }

  private def restoreFolders(logic: RestoredPathLogic, folders: Seq[FolderDescription]) = {
    for (folder <- folders) {
      val restoredFile = logic.makePath(folder.path)
      restoredFile.mkdirs()
      folder.applyAttrsTo(restoredFile)
    }
  }

  private def restoreFiles(logic: RestoredPathLogic, files: Seq[FileMetadata]): Unit = {
    logger.info(s"Restoring ${files.size} files")
    for (file <- files) {
      restoreFile(logic, file)
    }
  }

  private def restoreFile(logic: RestoredPathLogic, file: FileMetadata): Unit = {
    val fd = file.fd
    val restoredFile = logic.makePath(fd.path)
    restoredFile.getParentFile.mkdirs()
    if (restoredFile.exists()) {
      if (restoredFile.length() == fd.size && !fd.attrs.hasBeenModified(restoredFile)) {
        return
      }
      l.debug(s"${restoredFile.length()} ${fd.size} ${fd.attrs} ${restoredFile.lastModified()}")
      l.info("File exists, but has been modified, so overwrite")
    }
    val stream = new FileOutputStream(restoredFile)
    if (file.fd.size > 0) {
      val source = Source.fromIterator[Array[Byte]](() => file.blocks.grouped(config))
      Await.result(getBytestream(source).runForeach { x =>
        stream.write(x)
      }, 1.hour)
    }
    stream.close()
    FileAttributes.restore(fd.attrs, restoredFile)
  }
}

class RestoredPathLogic(val folders: Seq[FolderDescription], val restoreConf: RestoreConf) {

  sealed trait Result

  case object IsSubFolder extends Result

  case object IsTopFolder extends Result

  case object IsUnrelated extends Result

  case object IsSame extends Result

  def isRelated(folder: BackupPart, relatedTo: BackupPart): Result = {
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

  def detectRoots(folders: Iterable[BackupPart]): Seq[FolderDescription] = {
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

  def getRoot(sub: String): Option[FolderDescription] = {
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

    def cleaned(s: String) = if (Utils.isWindows) s.replaceAllLiterally(":", "_") else s

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
