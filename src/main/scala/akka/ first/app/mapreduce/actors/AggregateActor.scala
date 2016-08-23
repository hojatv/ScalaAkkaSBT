package akka.first.app.mapreduce.actors

import akka.actor.Actor
import akka.first.app.mapreduce.{ReduceData, Result}

import scala.collection.mutable.HashMap

/**
  * Created by hovaheb on 8/23/2016.
  */
class AggregateActor extends Actor {
  val finalReduceMap = new HashMap[String, Int]


  override def receive: Receive = {
    case ReduceData(reduceDataMap) => {
      aggregateInMemoryReduce(reduceDataMap)
    }
    case Result => {
      sender ! finalReduceMap.toString()
    }
  }

  /**
    * Aggregate actor receives the reduced data list from the Master actor and aggregates it into one big list.
    */
  /*
  private void aggregateInMemoryReduce(Map<String,
            Integer> reducedList) {
        Integer count = null;
        for (String key : reducedList.keySet()) {
            if (finalReducedMap.containsKey(key)) {
                count = reducedList.get(key) +
                        finalReducedMap.get(key);
                finalReducedMap.put(key, count);
            } else {
                finalReducedMap.put(key, reducedList.get(key));
            }
        }
    }
   */

  def aggregateInMemoryReduce(reducedList: Map[String, Int]): Unit = {
    for ((key, value) <- reducedList) {
      if (finalReduceMap contains key) {
        finalReduceMap(key) = (value + finalReduceMap.get(key).get)
      }
      else {
        finalReduceMap += (key -> value)
      }
    }
  }
}
