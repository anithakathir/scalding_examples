package scalding

import com.twitter.scalding._
import java.net.URL

class HostCountJob(args : Args) extends Job(args) {

  Tsv( args("input"), 'line ).read
    .map('line -> 'host) { url : String => new URL(url).getHost()}
    .groupBy('host) { _.size }
    .filter('size) { count:Int => count >= 5}
    .write( Tsv( args("output") ) )

}

object HostCountJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "scalding.HostCountJob",
    "--input", "src/main/resources/sampleurls.txt",
    "--output", "src/main/resources/scalding_output",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}
