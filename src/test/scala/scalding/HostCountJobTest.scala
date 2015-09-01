package scalding

import com.twitter.scalding.{TupleConversions, Tsv, JobTest}
import org.scalatest.{ShouldMatchers, FlatSpec}
import scala.collection.mutable.Buffer
import cascading.tuple.Fields


class HostCountJobTest extends FlatSpec with ShouldMatchers with TupleConversions{

  val fields = new Fields("url")

  "url list" should "be grouped by hostname" in {

    val validInput = "http://www.google.com/abc" :: "http://www.yahoo.com/def" :: "http://www.google.com/ghi" ::
      "http://www.yahoo.com/jkl" :: "http://www.yahoo.com/mno" :: "http://www.yahoo.com/pqr" ::
      "http://www.yahoo.com/stu" :: Nil

    def verify(buffer: Buffer[(String, Int)]) {

      buffer.size shouldEqual 1

      buffer(0)._1 should be ("www.yahoo.com")
      buffer(0)._2 should be (5)

    }

    JobTest(new HostCountJob(_))
      .arg("input", "inputFile")
      .arg("output", "outputFile")
      .source(Tsv("inputFile",fields), validInput)
      .sink[(String,Int)](Tsv("outputFile"))(verify)
      .run
      .finish
  }

}
