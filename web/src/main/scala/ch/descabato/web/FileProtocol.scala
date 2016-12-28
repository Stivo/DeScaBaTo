package ch.descabato.web

/**
  * Created by Stivo on 26.12.2016.
  */
object FileProtocol {
  import spray.json._

  case class Listing(action: String, path: String)

  case class GetContent(action: String, item: String)

  case class ListEntry(name: String, rights: String, size: Long, date: String, `type`: String)

  object Listing extends DefaultJsonProtocol {
    implicit val format = jsonFormat2(Listing.apply)
  }

  object GetContent extends DefaultJsonProtocol {
    implicit val format = jsonFormat2(GetContent.apply)
  }

  object ListEntry extends DefaultJsonProtocol {
    implicit val format = jsonFormat5(ListEntry.apply)
  }
  /*
  {
        "name": "magento",
        "rights": "drwxr-xr-x",
        "size": "4096",
        "date": "2016-03-03 15:31:40",
        "type": "dir"
    }
   */
}
