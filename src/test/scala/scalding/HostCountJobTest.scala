package scalding

import com.twitter.scalding.{TupleConversions, Tsv, JobTest}
import org.scalatest.{ShouldMatchers, FlatSpec}
import scala.collection.mutable.Buffer
import cascading.tuple.Fields


class HostCountJobTest extends FlatSpec with ShouldMatchers with TupleConversions{

  val fields = new Fields("host")

  "url list" should "be grouped by hostname" in {

    val validInput = "http://www.google.com/" :: "http://www.yahoo.com/" :: "http://www.google.com/" :: Nil

    def verify(buffer: Buffer[(String, Int)]) {

      buffer.size shouldEqual 2

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
