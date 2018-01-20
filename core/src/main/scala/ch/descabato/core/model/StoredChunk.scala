package ch.descabato.core.model

import ch.descabato.utils.Hash

case class StoredChunk(val file: String, val hash: Hash, val startPos: Long, val length: Length)
