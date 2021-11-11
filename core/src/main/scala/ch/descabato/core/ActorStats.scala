package ch.descabato.core

import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

object ActorStats {
  val tpe: ThreadPoolExecutor = {
    val out = new ThreadPoolExecutor(10, 10, 10, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable]())
    out.setThreadFactory((r: Runnable) => {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setPriority(2)
      t.setDaemon(true)
      t
    })
    out
  }

}
