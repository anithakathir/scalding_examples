package searchphrases


import com.twitter.scalding._
import RemoveInsignificantItems._

class SearchPhraseJob(args : Args) extends Job(args) {

  Tsv( args("input"), 'line ).read
    .flatMap('line -> 'phrase) {
    line: String => generatePossiblePhrases(line)
    }
    .groupBy('phrase) { _.size('count) }
    .groupBy('count) {_.sortBy('count).reverse}
    .limit(args("limit").toLong)
	  .project('count,'phrase)
    .write( Tsv( args("output") ) )


  def generatePossiblePhrases(text : String) : List[String] = {

    val wordList = text
      .replaceSymbols
      .removeSpecialCharacters
      .toLowerCase.split(" ")
      .toList
      .removePrepositions
      .removeEmptyWords

    val phraseList = (2 to wordList.size).foldLeft(wordList.combinations(1).toList)((accumulatedList, numWordsToTake) => accumulatedList ::: wordList.combinations(numWordsToTake).toList)

    phraseList.map( listOfWords => listOfWords.mkString(" "))
  }

}

object RemoveInsignificantItems
{
  val insignificantWordsList = List("with","on","of","the","a","an","and")
  val symbolExpansionMap = Map("\"" -> "Inch", "%" -> "Percentage")

  implicit class RemovePrepositions(list: List[String]) {

    implicit def removePrepositions: List[String] = list.filterNot(word => insignificantWordsList.contains(word))

    implicit def removeEmptyWords: List[String] = list.filterNot(word => word.isEmpty)

  }

  implicit class RemoveSpecialCharacters(str: String) {

    implicit def removeSpecialCharacters: String = str.replaceAll("[^A-Za-z0-9. ]","")

    implicit def replaceSymbols: String = symbolExpansionMap.foldLeft(str){case(text,(symbol,replacement)) => text.replace(symbol,replacement)}

  }
}


object SearchPhraseJob extends App {
  val progargs: Array[String] = List(
    "-Dmapred.map.tasks=200",
    "searchphrases.SearchPhraseJob",
    "--input", "src/main/resources/products.txt",
    "--output", "src/main/resources/PhraseCount",
    "--limit", "50",
    "--hdfs"
  ).toArray
  Tool.main(progargs)
}
