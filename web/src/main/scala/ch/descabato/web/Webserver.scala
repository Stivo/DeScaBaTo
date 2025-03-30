package ch.descabato.web

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.model.ChunkId
import ch.descabato.core.model.ChunkKey
import ch.descabato.core.model.FileMetadataId
import ch.descabato.core.model.RevisionKey
import ch.descabato.core.model.Size
import ch.descabato.core.util.FileManager
import ch.descabato.core.util.InMemoryDb
import ch.descabato.core.util.RetentionPolicyConfig
import ch.descabato.core.util.ValueLogReader
import ch.descabato.core.util.ValueLogWriter
import ch.descabato.core.util.VolumeUsageCounters
import ch.descabato.core.util.VolumeUsageReport
import ch.descabato.frontend.BackupConfCommandCreator
import ch.descabato.frontend.BackupFolderOption
import ch.descabato.protobuf.keys.RevisionValue
import ch.descabato.protobuf.keys.ValueLogIndex
import ch.descabato.utils.CompressedBytes
import ch.descabato.utils.JsonSerialization
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.typesafe.scalalogging.LazyLogging
import io.javalin.Javalin
import io.javalin.http.ContentType
import io.javalin.http.HttpStatus
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption

import java.awt.Desktop
import java.awt.Desktop.Action
import java.io.InputStream
import java.net.URI
import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.io.Source
import scala.util.Using


class WebServeCommand(serveConf: WebServeConf, backupConf: BackupFolderConfiguration) {

  def start(): Unit = {
    var r: Renderer = null

    def replaceRenderer(): Unit = {
      val backupEnv: BackupEnv = BackupEnv(backupConf, readOnly = serveConf.readOnly())
      r = new Renderer(backupEnv, backupConf)
    }

    replaceRenderer()
    var keepRunning = true
    val app = Javalin.create { config =>
        config
      }
      .delete("/api/revision/<id>", { ctx => r.deleteRevision(ctx.pathParam("id").toInt); ctx.status(HttpStatus.OK).result() })
      .get("/api/revisions", ctx => ctx.json(r.revisions()))
      .post("/api/retentionPolicy", { ctx => r.setRetentionPolicy(ctx.body()); ctx.status(200).result() })
      .post("/api/applyDeletions", { ctx =>
        val out = r.applyDeletion()
        replaceRenderer()
        ctx.json(out)
      })
      .get("/api/volume_usage", { ctx => ctx.json(r.volumeUsage("all")) })
      .post("/api/stopServer", { ctx => keepRunning = false; ctx.result() })
      .post("/api/rollback", { ctx => r.reset(); ctx.status(200).json("") })
      .get("/stats", ctx => ctx.json(r.stats()))
      .get("/index.html", ctx => ctx.html(r.getIndex()))
      .get("/<name>.html", ctx => ctx.contentType(ContentType.TEXT_HTML).result(r.getFile(ctx.pathParam("name") + ".html")))
      .get("/js/<name>", ctx => ctx.contentType(ContentType.JAVASCRIPT).result(r.getFile("js/" + ctx.pathParam("name"))))
      .get("/css/<name>", ctx => ctx.contentType(ContentType.CSS).result(r.getFile("css/" + ctx.pathParam("name"))))
      .get("/", ctx => ctx.redirect("/index.html"))
      .start(serveConf.port())
    if (serveConf.openBrowser() && Desktop.getDesktop().isSupported(Action.BROWSE)) {
      Desktop.getDesktop.browse(new URI(s"http://localhost:${serveConf.port()}"))
    }
    while (keepRunning) {
      Thread.sleep(1000)
    }
  }
}


class Renderer(backupEnv: BackupEnv, backupFolderConf: BackupFolderConfiguration) extends LazyLogging {

  private val kvs = backupEnv.rocks
  // TODO allow metadata compaction
  //    new ch.descabato.core.actions.DoMetadataCompaction(backupFolderConf).doCompaction()
  private val json = new JsonSerialization(true)

  private val lock = new Object
  private var copiedRevisions = kvs.getAllRevisions()

  def volumeUsage(revisionArg: String): String = {
    def impl(revisions: Seq[RevisionKey]) = {
      val chunkIds = getUsedChunks(revisions)
      json.mapper.writeValueAsString(getVolumeUsageReport(chunkIds))
    }

    if (revisionArg == "all") {
      impl(copiedRevisions.keys.toSeq)
    } else {
      val revision = revisionArg.toInt
      impl(Seq(RevisionKey(revision)))
    }
  }

  private def getUsedChunks(revisions: Seq[RevisionKey]): Set[ChunkId] = {
    val revisionValues = revisions.map { r =>
      kvs.readRevision(r).get
    }
    val fileIds: Seq[FileMetadataId] = revisionValues.flatMap(_.fileIdentifiers).distinct
    fileIds.toSet.flatMap { x =>
      kvs.getFileMetadataById(x).get._2.hashIds
    }
  }

  private def getVolumeUsageReport(chunkIds: Set[ChunkId]): Seq[VolumeUsageReport] = {
    val vucs = new VolumeUsageCounters(backupFolderConf)
    chunkIds.foreach { x =>
      vucs.addChunk(kvs.getChunkById(x).get._2)
    }
    vucs.reportAllVolumes()
  }

  def deleteRevision(id: Int): Unit = {
    lock.synchronized {
      copiedRevisions = copiedRevisions.filterNot(_._1.number == id)
    }
  }

  private val _fileIdSizeCache = mutable.Map.empty[FileMetadataId, Long]
  private val _revisionSizeCache = mutable.Map.empty[RevisionKey, Long]

  private var retentionPolicyConfig = RetentionPolicyConfig()

  def revisions(): String = {
    val keyToValue = copiedRevisions
    val tags: Map[RevisionValue, Seq[String]] = retentionPolicyConfig.applyTo(keyToValue.values.toSeq)
    val copy = keyToValue.toSeq.sortBy(_._1.number).reverse.map { ktv =>
      val totalSize = _revisionSizeCache.getOrElseUpdate(ktv._1,
        ktv._2.fileIdentifiers.map { fi =>
          _fileIdSizeCache.getOrElseUpdate(fi, kvs.getFileMetadataById(fi).get._2.length)
        }.sum
      )

      Map(
        "number" -> ktv._1.number,
        "timestamp" -> ktv._2.created,
        "number_of_files" -> ktv._2.fileIdentifiers.size,
        "total_size" -> totalSize,
        "date_time" -> LocalDateTime.ofEpochSecond(ktv._2.created / 1000, 0, ZoneOffset.UTC),
        "tags" -> tags.getOrElse(ktv._2, Seq.empty)
      )
    }
    json.mapper.registerModule(new JavaTimeModule()).writer().writeValueAsString(copy)
  }

  def stats(): String = {
    val stats = Map(
      "revisions" -> kvs.getAllRevisions().size,
      "chunks" -> kvs.getAllChunks().size,
      "chunks_size" -> kvs.getAllChunks().map(_._2.lengthCompressed).sum,
      "files" -> kvs.getAllFileMetadata().size,
      "size" -> kvs.getAllFileMetadata().map(_._2.length).sum,
    )
    json.mapper.writeValueAsString(stats)
  }

  def getFile(x: String): InputStream = {
    this.getClass.getResourceAsStream("/web/" + x)
  }

  def reset(): String = {
    copiedRevisions = kvs.getAllRevisions()
    ""
  }

  def applyDeletion(): String = {
    var out = Map.empty[String, Int]
    val toDeleteRevisions = (kvs.getAllRevisions() -- copiedRevisions.keys).toSeq
    var showCopy = toDeleteRevisions.map {
      x => x._1
    }
    if (toDeleteRevisions.isEmpty) {
      println(s"No revisions pending to delete")
      return ""
    }
    println(s"Deleting these revisions ${showCopy}")
    toDeleteRevisions.foreach { x =>
      kvs.deleteRevision(x._1)
    }
    val usedFileIdentifiers = copiedRevisions.toSet.flatMap(_._2.fileIdentifiers)
    kvs.deleteOtherFileMetadataIds(usedFileIdentifiers)
    // delete chunks
    Using.Manager { use =>
      val valueLog = use(new ValueLogWriter(backupEnv, backupEnv.fileManager.volume, write = true, backupEnv.config.volumeSize.bytes))
      val reader: ValueLogReader = use(new ValueLogReader(backupEnv))
      val usedChunks: Set[ChunkId] = getUsedChunks(copiedRevisions.keys.toSeq)
      val fm = new FileManager(backupFolderConf)

      getVolumeUsageReport(usedChunks).sortBy(_.unusedBytes).reverse.takeWhile(_.unusedBytes > 20 * 1024 * 1024).foreach { volume =>
        val allChunksThisVolume: Seq[(ChunkKey, ValueLogIndex)] = kvs.getAllChunks().filter(_._2.filename.endsWith(volume.volume)).toSeq
        val (usedChunksThisVolume, unusedChunksThisVolume) = allChunksThisVolume.partition(x => usedChunks.contains(kvs.getChunk(x._1).get._1))
        val usedBytes = usedChunksThisVolume.map(_._2.lengthCompressed).sum
        val unusedBytes = unusedChunksThisVolume.map(_._2.lengthCompressed).sum
        out += volume.volume -> (unusedChunksThisVolume.map(_._2.lengthCompressed).sum)
        usedChunksThisVolume.foreach { case (k, v) =>
          val toWrite = reader.readValue(v, alsoDecompress = false)
          val (result, newValue) = valueLog.write(CompressedBytes(toWrite, v.lengthUncompressed))
          kvs.writeChunk(k, newValue)
        }
        unusedChunksThisVolume.foreach { case (k, _) =>
          kvs.deleteChunk(k)
        }
        println(s"Rewriting ${volume.volume}, keeping ${Size(usedBytes)}, discarding ${Size(unusedBytes)}")
      }
    }.get
    InMemoryDb.writeFile(backupEnv, kvs.getUpdates())
    println(s"Revision deletion finished")
    json.mapper.writeValueAsString(out)
  }

  def setRetentionPolicy(jsonInput: String): String = {
    retentionPolicyConfig = json.mapper.readValue(jsonInput, classOf[RetentionPolicyConfig])
    ""
  }

  def getIndex(): String = {
    val is = getFile("/index.html")
    val content = Source.fromInputStream(is, "UTF-8").getLines().mkString("\n")
    content.replace("{{revisionsDataHere}}", revisions())
  }

}

class WebServeConf(args: Seq[String]) extends ScallopConf(args) with BackupFolderOption
  with BackupConfCommandCreator {

  import org.rogach.scallop.flagConverter
  import org.rogach.scallop.intConverter

  val port: ScallopOption[Int] = opt[Int](descr = "The port to host the server on", required = false, default = Some(7070))
  val readOnly: ScallopOption[Boolean] = opt[Boolean](descr = "Open this readOnly, so no modifications are accidentally made", required = false, default = Some(true))
  val openBrowser: ScallopOption[Boolean] = opt[Boolean](descr = "Opens the browser", required = false, default = Some(true))

  override def runCommand(backupFolderConf: BackupFolderConfiguration): Unit =
    new WebServeCommand(this, backupFolderConf).start()

  override def needsExistingBackup: Boolean = true
}
