package ch.descabato.core.model

import ch.descabato.core_old.FileDescription
import ch.descabato.utils.Hash

case class FileMetadata(fd: FileDescription, blocks: Hash)
