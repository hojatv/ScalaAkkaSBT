package akka.first.app.mapreduce.actors

import akka.actor.Actor
import akka.actor.ActorRef
import akka.first.app.mapreduce.MapData
import akka.first.app.mapreduce.WordCount
import scala.collection.mutable.ArrayBuffer

class MapActor extends Actor {
  val STOP_WORDS_LIST = List("a", "am", "an", "and", "are", "as",
    "at", "be", "do", "go", "if", "in", "is", "it", "of", "on", "the",
    "to")
  val defaultCount: Int = 1

  def receive: Receive = {
    case message: String =>
      sender ! evaluateExpression(message)
  }

  //logic to map the words in the sentences
  /*List<WordCount> dataList = new ArrayList<WordCount>();
  StringTokenizer parser = new StringTokenizer(line);
  while (parser.hasMoreTokens()) {
    String word = parser.nextToken().toLowerCase();
    if (!STOP_WORDS_LIST.contains(word)) {
      dataList.add(new WordCount(word, Integer.valueOf(1)));
    }
  return new MapData(dataList);
  }*/

  def evaluateExpression(line: String): MapData = MapData {
    line.split("""\s+""").foldLeft(ArrayBuffer.empty[WordCount]) {
      (index, word) => if (!STOP_WORDS_LIST.contains(word.toLowerCase))
        index += WordCount(word.toLowerCase, 1)
      else
        index
    }
  }
}