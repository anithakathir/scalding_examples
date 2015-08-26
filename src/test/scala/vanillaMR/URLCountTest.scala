package vanillaMR

import org.scalatest.{ShouldMatchers, FlatSpec}

import scala.io.Source


class URLCountTest extends FlatSpec with ShouldMatchers{


  URLCount.countHosts("/src/main/resources/sampleurls.txt","/src/main/resources/inputurls.txt","src/main/resources/output.txt")

  Source.fromFile("src/main/resources/output.txt") shouldBe("www.itpartsdepot\t8")


}
