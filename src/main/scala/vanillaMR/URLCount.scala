package vanillaMR

import scala.io.Source
import java.io._

object URLCount {

  def countHosts(inputURLs: String, outputFile: String) {

    val url = Source.fromFile(inputURLs).getLines().toList.groupBy((url: String) => new java.net.URL(url).getHost).mapValues(_.length)

    val fileWriter = new PrintWriter(new File(outputFile))

    url.foreach{case(key,value) => if(value >= 5) fileWriter.write(key+"\t"+value+"\n")}

    fileWriter.close()

  }

}