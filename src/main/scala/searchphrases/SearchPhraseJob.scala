package searchphrases


import com.twitter.scalding._

class SearchPhraseJob(args : Args) extends Job(args) {

  Tsv( args("input"), 'line ).read
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size('count) }
    .groupBy('count) {_.sortBy('count).reverse}
    .write( Tsv( args("output") ) )

  def tokenize(text : String) : Array[String] = {
    text.toLowerCase.split(" ")
  }

}

object SearchPhraseJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "searchphrases.SearchPhraseJob",
    "--input", "src/main/resources/products.txt",
    "--output", "src/main/resources/PhraseCount",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}
