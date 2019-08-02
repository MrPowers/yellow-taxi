package mrpowers.yellow.taxi

import org.scalatest.FunSpec

class LakeCreatorSpec extends FunSpec {

  it("creates a bunch of lakes") {
    LakeCreator.createParquetLake()
  }

}
