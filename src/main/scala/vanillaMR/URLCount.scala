package vanillaMR

import scala.io.Source
import java.io._

object URLCount {

  def countHosts(sampleURLs: String, inputURLs: String, outputFile: String) {

    val sample = Source.fromFile(sampleURLs).getLines().toSet
    val input = Source.fromFile(inputURLs).getLines().toSet

    val url = sample.intersect(input).toList.groupBy((url: String) => new java.net.URL(url).getHost).mapValues(_.length)

    val fileWriter = new PrintWriter(new File(outputFile))

    url.foreach{case(key,value) => if(value >= 5) fileWriter.write(key+"\t"+value)}

    fileWriter.close()

  }

}