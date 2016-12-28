package ch.descabato.web

import java.awt.Desktop
import java.io._
import java.util.{Date, Locale}
import javafx.scene.{control => jfxc}

import scalafx.Includes._
import ch.descabato.core.{FileDescription, Size}

import scalafx.scene.control.{TableCell, TableColumn, TableRow, TableView}
import scalafx.scene.control.cell.TextFieldTableCell
import scalafx.scene.image.{Image, ImageView}
import scalafx.util.converter.{DateStringConverter, LongStringConverter}

/**
  * Created by Stivo on 26.12.2016.
  */
object TableColumns {

  def stream(inputStream: InputStream, outputStream: OutputStream) = {
    val buffer = new Array[Byte](16384)

    def doStream(total: Int = 0): Int = {
      val n = inputStream.read(buffer)
      if (n == -1)
        total
      else {
        outputStream.write(buffer, 0, n)
        doStream(total + n)
      }
    }

    doStream()
  }

  def createRowFactory(table: TableView[ObservableBackupPart], model: BackupViewModel) = {
    tv: TableView[ObservableBackupPart] =>
      val row = new TableRow[ObservableBackupPart]();
      row.onMouseClicked = { event =>
        if (event.getClickCount() == 2 && (!row.isEmpty())) {
          val rowData: ObservableBackupPart = row.getItem()
          if (rowData.backupPart.isFolder) {
            //            model.populateListForPath(rowData.backupPart)
            //            val treenodeForPath: Option[TreeItem[BackupTreeNode]] = {
            //              var path = rowData.backupPart.pathParts
            //              var item: Option[TreeItem[BackupTreeNode]] = Some(treeView.root.value)
            //              while(!path.isEmpty && item.isDefined) {
            //                val children: ObservableList[TreeItem[BackupTreeNode]] = item.get.getChildren
            //                item = children.find{x: TreeItem[BackupTreeNode] => x.value.value.backupPart.name == path.head}
            //                path = path.tail
            //              }
            //              item
            //            }
            //            treenodeForPath match {
            //              case Some(node) => treeView.selectionModel().select(node)
            //              case None => println("could not select "+rowData.backupPart)
            //            }
          } else {
            val value = rowData.name.value
            val extension = "." + value.reverse.takeWhile(_ != '.').reverse
            val tempFile = File.createTempFile(value, extension)
            tempFile.deleteOnExit()
            val fos = new FileOutputStream(tempFile)
            val in = model.index.getInputStream(rowData.backupPart.asInstanceOf[FileDescription])
            stream(in, fos)
            fos.close()
            Desktop.getDesktop.open(tempFile)
          }
        }
      }
      row
  }

  def initTable(table: TableView[ObservableBackupPart], model: BackupViewModel): Unit = {
    initNameColumn(table)
    initPathColumn(table)
    initDateColumn(table)
    initSizeColumn(table)
    initTypeColumn(table)

    table.rowFactory = createRowFactory(table, model)
  }

  private def initSizeColumn(table: TableView[ObservableBackupPart]) = {
    findColumn[Long]("size", table).foreach { c =>
      c.cellValueFactory = {
        _.value.size
      }
      c.cellFactory = { _: TableColumn[ObservableBackupPart, Long] =>
        new TextFieldTableCell[ObservableBackupPart, Long](new LongStringConverter() {
          override def toString(s: Long): String = Size(s).toString
        })
      }
    }
  }

  private def initDateColumn(table: TableView[ObservableBackupPart]): Unit = {
    findColumn[Date]("date", table).foreach { c =>
      c.cellValueFactory = {
        _.value.date
      }
      c.cellFactory = { _: TableColumn[ObservableBackupPart, Date] =>
        new TextFieldTableCell[ObservableBackupPart, Date](new DateStringConverter(Locale.getDefault))
      }
    }
  }

  def findColumn[T](name: String, tableView: TableView[ObservableBackupPart])
  : Option[jfxc.TableColumn[ObservableBackupPart, T]] = {
    tableView.columns.find(_.getId == name).map {
      case c: jfxc.TableColumn[ObservableBackupPart, T] =>
        c
    }
  }

  private def initPathColumn(table: TableView[ObservableBackupPart]): Unit = {
    findColumn[String]("path", table).foreach { c =>
      c.cellValueFactory = {
        _.value.path
      }
    }
  }

  private def initNameColumn(tableView: TableView[ObservableBackupPart]): Unit = {
    findColumn[String]("name", tableView).foreach { c =>
      c.cellValueFactory = {
        _.value.name
      }
    }
  }

  private def initTypeColumn(table: TableView[ObservableBackupPart]): Unit = {
    findColumn[String]("type", table).foreach { c =>
      def getIcon(icon: String): Image = {
        val resource = new File("C:\\Users\\Stivo\\workspace\\DeScaBaTo\\web\\src\\resources", icon)
        new Image(new FileInputStream(resource))
      }

      val folderIcon = getIcon("Folder.png")
      val fileIcon = getIcon("File.png")

      c.cellValueFactory = {
        _.value.`type`
      }

      c.cellFactory = {
        _: TableColumn[ObservableBackupPart, String] =>
          new TableCell[ObservableBackupPart, String] {
            item.onChange { (_, _, newType) =>
              graphic = {
                val newView = if (newType == "dir")
                  Some(new ImageView(folderIcon))
                else if (newType == "file")
                  Some(new ImageView(fileIcon))
                else
                  None
                newView.foreach { view =>
                  view.fitHeight = 16
                  view.fitWidth = 16
                }
                newView.orNull
              }
            }
          }
      }
    }
  }

}
