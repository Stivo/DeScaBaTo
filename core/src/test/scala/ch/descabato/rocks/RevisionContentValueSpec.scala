package ch.descabato.rocks


import ch.descabato.utils.Implicits._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class RevisionContentValueSpec extends WordSpec {

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
