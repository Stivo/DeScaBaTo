package ch.descabato.ui

import ch.descabato.core.{FileDescription, Size}
import ch.descabato.utils.Utils

import scala.io.Source
import scalafx.beans.binding.Bindings
import scalafx.scene.control._
import scalafx.scene.image.{Image, ImageView}
import scalafx.scene.layout.{BorderPane, FlowPane}
import scalafxml.core.macros.sfxml

trait PreviewControllerI extends ChildController {
  def preview(fileDescription: FileDescription): Unit
}

sealed trait PreviewType
case object Image extends PreviewType
case object Text extends PreviewType
case object Unknown extends PreviewType

@sfxml
class PreviewController(
                         val infoLabel: Label,
                         val imageView: ImageView,
                         val borderPane: BorderPane,
                         val textView: TextArea
                       ) extends PreviewControllerI with Utils {

  val imageScrollPane = new ScrollPane()
  borderPane.center = imageScrollPane
  imageScrollPane.maxWidth = Double.MaxValue
  imageScrollPane.maxHeight = Double.MaxValue
  imageScrollPane.content = imageView

  val textFilesRegex = ("(?i)\\.(csv|txt|diff?|patch|svg|asc|cnf|cfg|conf|html?|.html|cfm|cgi|aspx?|ini|pl|py" +
    "|md|css|cs|js|jsp|log|htaccess|htpasswd|gitignore|gitattributes|env|json|atom|eml|rss|markdown|sql|xml|xslt?|" +
    "sh|rb|as|bat|cmd|cob|for|ftn|frm|frx|inc|lisp|scm|coffee|php[3-6]?|java|c|cbl|go|h|scala|vb|tmpl|lock|go|yml|yaml|tsv|lst)$").r
  val isImageFilePattern = "(?i)\\.(jpe?g|gif|bmp|png|svg|tiff?)$".r

  def preview(fileDescription: FileDescription, contentType: PreviewType): Unit = {
    contentType match {
      case Text => previewText(fileDescription)
      case Image => previewImage(fileDescription)
      case Unknown => previewUnknown(fileDescription)
    }
  }

  def previewUnknown(fileDescription: FileDescription) = {
    infoLabel.text = s"Can not preview ${fileDescription.path} ${new Size(fileDescription.size)}"
    val flowPane = new FlowPane() {
      children = Seq(
        new Button("Try as Text") {
          onAction = { _ =>
            preview(fileDescription, Text)
          }
        },
        new Button("Try as Image") {
          onAction = { _ =>
            preview(fileDescription, Image)
          }
        }
      )
    }
    borderPane.center = flowPane
  }

  def preview(fileDescription: FileDescription): Unit = {
    l.info(s"Previewing ${fileDescription.path}")
    if (textFilesRegex.findFirstIn(fileDescription.path).isDefined) {
      preview(fileDescription, Text)
    } else if (isImageFilePattern.findFirstIn(fileDescription.path).isDefined) {
      preview(fileDescription, Image)
    } else {
      preview(fileDescription, Unknown)
    }
  }

  private def previewImage(fileDescription: FileDescription) = {
    infoLabel.text = s"Previewing ${fileDescription.path} ${new Size(fileDescription.size)} as image"
    l.info(s"Previewing as image file")
    // image file
    FxUtils.runInBackgroundThread {
      val stream = BackupViewModel.index.getInputStream(fileDescription)
      try {
        val image = new Image(stream)
        FxUtils.runInUiThread {
          imageView.image = image
          val property = Bindings.createDoubleBinding( { () =>
            val widthProperty: Double = imageScrollPane.width()
            Math.min(widthProperty, image.width())
          }, imageScrollPane.width)
          imageView.fitWidthProperty().bind(property)

          borderPane.center = imageScrollPane
        }
      } finally {
        stream.close()
      }
    }
  }

  private def previewText(fileDescription: FileDescription) = {
    l.info(s"Previewing as text file")
    // text file
    infoLabel.text = s"Previewing ${fileDescription.path} (${fileDescription.sizeFormatted}) as text file"
    FxUtils.runInBackgroundThread {
      val stream = Source.fromInputStream(BackupViewModel.index.getInputStream(fileDescription))
      val lines = stream.getLines()
      val text = lines.take(10000).mkString("\n")
      val cut = !lines.isEmpty
      stream.close()
      l.info(s"Text loaded in background, displaying")
      FxUtils.runInUiThread {
        if (cut) {
          infoLabel.text = infoLabel.text() + ". Only showing first 10000 lines."
        }
        textView.text = text
        borderPane.center = textView
      }
    }
  }
}