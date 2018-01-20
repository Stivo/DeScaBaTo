package ch.descabato.core.model

import java.io.File

case class FileDescription(val path: String, val size: Length, val lastModified: Long) {

  def this(file: File) = this(file.getCanonicalPath, Length(file.length()), file.lastModified())

}

