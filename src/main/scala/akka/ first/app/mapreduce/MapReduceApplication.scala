package akka.first.app.mapreduce

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props, actorRef2Scala}
import akka.first.app.mapreduce.actors.MasterActor
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await;


sealed trait MapReduceMessage

case class WordCount(word: String, count: Int) extends
  MapReduceMessage

case class MapData(dataList: ArrayBuffer[WordCount]) extends
  MapReduceMessage

case class ReduceData(reduceDataMap: Map[String, Int]) extends
  MapReduceMessage

case class Result() extends MapReduceMessage

object MapReduceApplication extends App {
  val _system = ActorSystem("MapReduceApp")
  val master = _system.actorOf(Props[MasterActor], name = "master")
  implicit val timeout = Timeout(10, TimeUnit.SECONDS)
  master ! "The quick brown fox tried to jump over the lazy dog and  fell on the dog "
  master ! "Dog is man's best friend"
  master ! "Dog and Fox belong to the same family"
  Thread.sleep(5000)
  val future = (master ? Result).mapTo[String]
  val result = Await.result(future, timeout.duration)
  println(result)
  _system.terminate()
}