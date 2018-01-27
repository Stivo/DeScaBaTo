package ch.descabato.core.model

import ch.descabato.core_old.FileDescription
import ch.descabato.utils.Hash

case class FileMetadata(id: Long, fd: FileDescription, blocks: Hash)
