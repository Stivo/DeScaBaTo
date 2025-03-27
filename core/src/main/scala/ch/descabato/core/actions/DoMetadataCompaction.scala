package ch.descabato.core.actions

import ch.descabato.core.config.BackupFolderConfiguration
import ch.descabato.core.model.BackupEnv
import ch.descabato.core.util.InMemoryDb

class DoMetadataCompaction(conf: BackupFolderConfiguration) {

  def doCompaction(): Unit = {
    // TODO for this:
    // Rename old files, maybe delete them later?
    val backupEnv = BackupEnv(conf, readOnly = true)
    val proto = backupEnv.rocks.getAsProtoDb()
    InMemoryDb.writeFile(backupEnv, proto)
  }

}
