package scaldingjoins

import cascading.pipe.joiner.LeftJoin
import com.twitter.scalding._
import java.net.URL

class HostAddressFilterJob(args : Args) extends Job(args) {

  val hostWithCountPipe = TextLine( args("input_host_count") )
    .map('line -> ('host,'count)) { line : String => (line.split("\t")(0),line.split("\t")(1)) }
    .project('host,'count)

  val hostWithAddressPipe = TextLine( args("input_host_address") )
    .map('line -> ('host_addr, 'address)){line: String => (line.split("\t")(0), line.split("\t")(1))}
    .project('host_addr, 'address)

  val leftJoin = hostWithCountPipe
	  .joinWithSmaller('host -> 'host_addr,hostWithAddressPipe, joiner = new LeftJoin)
	  .project('host, 'count, 'address)
		.write(Tsv( args("output_left_join") ) )

	val innerJoin = hostWithCountPipe
		.joinWithSmaller('host -> 'host_addr,hostWithAddressPipe)
		.project('host, 'count, 'address)
		.write(Tsv( args("output_inner_join") ) )

}


object HostAddressFilterJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "scaldingjoins.HostAddressFilterJob",
    "--input_host_count", "src/main/resources/HostsWithCount.txt",
    "--input_host_address", "src/main/resources/HostsWithAddress.txt",
    "--output_left_join", "src/main/resources/scalding_left_join_output",
	  "--output_inner_join", "src/main/resources/scalding_inner_join_output",
	  "--hdfs"
  ).toArray
  Tool.main(progargs)
}