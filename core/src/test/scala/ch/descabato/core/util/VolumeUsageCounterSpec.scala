package ch.descabato.core.util

import ch.descabato.protobuf.keys.ValueLogIndex
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.wordspec.AnyWordSpec

class VolumeUsageCounterSpec extends AnyWordSpec {

  "volume" in {
    val vuc = new VolumeUsageCounter("test", 10000)

    vuc.reportUnusedBytes() should be(10000)

    vuc.addChunk(ValueLogIndex("test", 500, 500, 500))

    vuc.reportUnusedBytes() should be(9500)

    vuc.addChunk(ValueLogIndex("test", 1000, 500, 500))

    vuc.reportUnusedBytes() should be(9000)

    vuc.addChunk(ValueLogIndex("test", 2000, 500, 500))

    vuc.reportUnusedBytes() should be(8500)
  }

}
