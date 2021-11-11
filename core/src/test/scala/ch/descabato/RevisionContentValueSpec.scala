package ch.descabato

import ch.descabato.core.model.RevisionContentValue
import ch.descabato.utils.Implicits._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

class RevisionContentValueSpec extends AnyWordSpec {

  "Revision content encoding" should {
    "encode and decode correctly for an update" in {
      val value1 = RevisionContentValue.createUpdate(7, "test".getBytes, "test2".getBytes().wrap)
      val encoded = value1.asArray()
      val decodedValue = RevisionContentValue.readNextEntry(encoded.asInputStream())
      decodedValue should equal(Some(value1))
    }

    "encode and decode correctly for a delete" in {
      val value1 = RevisionContentValue.createDelete(7, "test".getBytes)
      val encoded = value1.asArray()
      val decodedValue = RevisionContentValue.readNextEntry(encoded.asInputStream())
      decodedValue should equal(Some(value1))
    }
  }

}
