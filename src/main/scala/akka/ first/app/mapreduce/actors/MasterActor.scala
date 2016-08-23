package akka.first.app.mapreduce.actors

import akka.actor.{Actor, Props}
import akka.first.app.mapreduce._
import akka.routing.RoundRobinPool

class MasterActor extends Actor {
  val mapActor = context.actorOf(Props[MapActor].withRouter(
    RoundRobinPool(nrOfInstances = 5)), name = "map")
  val reduceActor = context.actorOf(Props[ReduceActor].withRouter(
    RoundRobinPool(nrOfInstances = 5)), name = "reduce")
  val aggregateActor = context.actorOf(Props[AggregateActor],
    name = "aggregate")

  override def receive: Receive = {
    case line: String =>
      mapActor ! line
    case mapData: MapData =>
      reduceActor ! mapData
    case reduceData: ReduceData => {
      aggregateActor ! reduceData
    }
    case Result =>
      aggregateActor forward Result
  }
}