package ch.descabato.core.model

import akka.util.ByteString
import ch.descabato.utils.{BytesWrapper, Hash}

case class FileMetadata(fd: FileDescription, blocks: Hash)
