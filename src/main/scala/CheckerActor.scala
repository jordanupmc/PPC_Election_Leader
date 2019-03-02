package upmc.akka.leader

import java.util
import java.util.Date

import akka.actor._
import scala.math.Ordering.Implicits._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

abstract class Tick
case class CheckerTick () extends Tick

class CheckerActor (val id:Int, val terminaux:List[Terminal], electionActor:ActorRef) extends Actor {

     var time : Int = 500
     val father = context.parent

     var nodesAlive:List[Int] = List()
     var datesForChecking:List[Date] = List()
     var lastDate:Date = null

     var leader : Int = -1
     //JOJO
     var mapNodeDate:Map[Int, Date] = Map() 
     //nombre de Beat avant d'etre considerer mort
     var lifeOrDeadMirror : Int = 3

    def receive = {

         // Initialisation
        case Start => {
             self ! CheckerTick
        }

        // A chaque fois qu'on recoit un Beat : on met a jour la liste des nodes
        case IsAlive (nodeId) => {
          mapNodeDate = mapNodeDate + (nodeId -> new Date)
        }

        case IsAliveLeader (nodeId) => {
          if(this.leader != nodeId){
               //TODO hack
               electionActor ! RebootState
          }
          this.leader = nodeId;
          mapNodeDate = mapNodeDate + (nodeId -> new Date)
        }
        // A chaque fois qu'on recoit un CheckerTick : on verifie qui est mort ou pas
        // Objectif : lancer l'election si le leader est mort
        case CheckerTick => {
          context.system.scheduler.scheduleOnce(new FiniteDuration(time, MILLISECONDS), self ,CheckerTick)

          //Ensemble des noeud mort
          var setDead = mapNodeDate.filter( {
               case (key, date) => Math.abs(date.getTime() - lastDate.getTime()) >=  lifeOrDeadMirror * time 
          }).keys

          //Delete les noeud mort de la map
          mapNodeDate --= setDead

          var leadDead =false

          setDead.foreach({
               key => if(key == leader)leadDead=true; else this.father ! Message("Node "+key+ " is dead")
          })

          if(leadDead){
               leader = -1
               father ! Message("LEADER is dead => ELECTION")
               electionActor ! StartWithNodeList(mapNodeDate.keys.toList)
          }

          lastDate = new Date
        }

    }


}
