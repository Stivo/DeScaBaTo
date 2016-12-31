package ch.descabato.web

import java.net.URLDecoder

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.directives.BasicDirectives.extractUnmatchedPath
import akka.http.scaladsl.server.directives.ContentTypeResolver
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.StreamConverters
import ch.descabato.core.FileDescription
import org.webjars.WebJarAssetLocator

import scala.io.StdIn

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
        pathPrefix("preview") {
          extractUnmatchedPath { unmatchedPath => requestContext =>
            getContent(URLDecoder.decode(unmatchedPath.toString(), "UTF-8"), requestContext)
          }
        } ~
        pathPrefix("size") {
          extractUnmatchedPath { unmatchedPath => requestContext =>
            getSize(URLDecoder.decode(unmatchedPath.toString(), "UTF-8"), requestContext)
          }
        }
      } ~ {
          getFromDirectory(staticPathIndex)
      }


    val bindingFuture = Http().bindAndHandle(route, "0.0.0.0", 8080)
    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFuture
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done
  }


  def getContent(path: String, requestContext: RequestContext) = {
    println ("Getting content of "+path)
    val node = index.tree.lookup(path.split('/').tail)
    node.backupPart match {
      case f : FileDescription =>

        val source = StreamConverters.fromInputStream(() => index.getInputStream(f))
//        val content = Source.fromInputStream(index.getInputStream(f)).getLines().mkString("\n")
        val contentType = ContentTypeResolver.Default(f.name)
        requestContext.complete(HttpEntity.apply(contentType, source))
      case _ =>
        requestContext.complete(StatusCodes.NotFound)
    }
  }

  def getSize(path: String, requestContext: RequestContext) = {
    println ("Getting content of "+path)
    val node = index.tree.lookup(path.split('/').tail)
    requestContext.complete(""+node.backupPart.size)
  }

}
