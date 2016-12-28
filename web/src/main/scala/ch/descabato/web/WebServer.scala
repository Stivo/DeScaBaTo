package ch.descabato.web

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Path

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.headers.{ContentDispositionTypes, `Content-Disposition`}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.directives.BasicDirectives.extractUnmatchedPath
import akka.http.scaladsl.server.directives.ContentTypeResolver
import akka.http.scaladsl.server.directives.RouteDirectives.complete
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.StreamConverters
import ch.descabato.core.{BackupPart, FileDescription}
import ch.descabato.web.FileProtocol.{GetContent, ListEntry, Listing}
import org.webjars.WebJarAssetLocator

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._

import scala.io.{Source, StdIn}

/**
  * Created by Stivo on 26.12.2016.
  */
object WebServer {

  val staticPath = "C:\\Users\\Stivo\\workspace\\angular-filemanager"
  val staticPathIndex = "C:\\Users\\Stivo\\workspace\\Descabato\\web"
  var backup = "e:/backup/pics"

  var index: Index = null

  val webjarAssets = new WebJarAssetLocator()

  def main(args: Array[String]) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val route =
      pathPrefix("api") {
        path("list") {
          post {
            entity(as[Listing]) { listing =>
              requestContext =>
                listEntries(listing, requestContext)
            }
          }
        } ~
          path("getContent") {
            post {
              entity(as[GetContent]) { getContentRequest =>
                requestContext =>
                  getContent(getContentRequest, requestContext)
              }
            }
          } ~
          path("downloadFile") {
            parameters('action, 'path) { (action, filePath) =>

              val contentType = ContentTypeResolver.Default(pathParts(filePath).last)
              val node = index.tree.lookup(pathParts(filePath).tail)
              node.backupPart match {
                case f : FileDescription =>
                  respondWithHeader(`Content-Disposition`(ContentDispositionTypes.attachment, Map(("filename", f.name)))) {
                    complete {
                      HttpEntity.Default(contentType, f.size,
                        StreamConverters.fromInputStream(() => index.getInputStream(f)))
                    }
                  }
                case _ =>
                  complete(StatusCodes.NotFound)
              }

             }
          }
      } ~
        pathEndOrSingleSlash {
          getFromFile(staticPathIndex + "/index.html")
        } ~ {

        extractUnmatchedPath { (unmatchedPath: Path) ⇒
          if (unmatchedPath.toString().contains(".min.")) {
            val string = unmatchedPath.toString()
            val fullPath = webjarAssets.getFullPath(string)
            getFromResource(fullPath)
          } else
            getFromDirectory(staticPath)
        }
      }


    //    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 8080)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }

  // 2016-03-03 15:31:40
  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def doGetLastModifiedTime(backupPart: BackupPart): Date = {
    if (backupPart.attrs != null) {
      new Date(backupPart.attrs.get("lastModifiedTime").toString.toLong)
    } else {
      new Date()
    }
  }

  def pathParts(x: Any): Seq[String] = x match {
    case x: String => x.split("[\\/]")
    case Some(x: BackupPart) => x.path.split("[\\/]")
    case x => println("Did not match " + x); Nil
  }

  def isPathChar(x: Char) = x == '/' || x == '\\'

  def nextPathPart(child: BackupPart, parent: BackupPart): String = child.path.drop(parent.path.length()).dropWhile(isPathChar).takeWhile(!isPathChar(_))

  def listEntries(listing: Listing, requestContext: RequestContext) = {
    println (listing.path)
    val entry = if (listing.path == "/") index.tree else index.tree.lookup(pathParts(listing.path).tail)
    val entries = entry.children.toSeq.sortBy(_._1).map { case (name, BackupTreeNode(part, _)) =>
      ListEntry(
        name,
        if (part.isFolder) "dr--r--r--" else "-r--r--r--",
        part.size,
        format.format(doGetLastModifiedTime(part)),
        if (part.isFolder) "dir" else "file"
      )
    }
    requestContext.complete(StatusCodes.OK, wrap(entries))
  }

  def getContent(getContentRequest: GetContent, requestContext: RequestContext) = {
    println (getContentRequest.item)
    val node = index.tree.lookup(pathParts(getContentRequest.item).tail)
    node.backupPart match {
      case f : FileDescription =>
        val content = Source.fromInputStream(index.getInputStream(f)).getLines().mkString("\n")
        requestContext.complete(StatusCodes.OK, wrap(content))
      case _ =>
        requestContext.complete(StatusCodes.NotFound)
    }
  }

  /*
  def listEntries(listing: Listing, requestContext: RequestContext) = {

    val toList = new File(filesPath + listing.path)
    val entries = toList.listFiles().map { file =>
      val rights = (if (file.isDirectory) "d" else "-") + "r--r--r--"
      ListEntry(file.getName,
        rights,
        file.length(),
        format.format(new Date(file.lastModified())),
        if (file.isDirectory) "dir" else "file")
    }

    requestContext.complete(StatusCodes.OK, wrap(entries))
  }

  def getContent(getContentRequest: GetContent, requestContext: RequestContext) = {
    val file = new File(filesPath + getContentRequest.item)
    val content = Source.fromFile(file).getLines().mkString("\n")
    requestContext.complete(StatusCodes.OK, wrap(content))
  }

*/
  private def wrap[T](t: T) = {
    Map("result" -> t)
  }

}
