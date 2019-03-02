package upmc.akka.leader

import akka.actor._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.util.Timeout

abstract class NodeStatus
case class Passive () extends NodeStatus
case class Candidate () extends NodeStatus
case class Dummy () extends NodeStatus
case class Waiting () extends NodeStatus
case class Leader () extends NodeStatus

abstract class LeaderAlgoMessage
case class Initiate () extends LeaderAlgoMessage
case class ALG (list:List[Int], nodeId:Int) extends LeaderAlgoMessage
case class AVS (list:List[Int], nodeId:Int) extends LeaderAlgoMessage
case class AVSRSP (list:List[Int], nodeId:Int) extends LeaderAlgoMessage
case class RebootState () extends LeaderAlgoMessage
//case class SyncStartMsg () extends LeaderAlgoMessage

case class StartWithNodeList (list:List[Int])


class ElectionActor (val id:Int, val terminaux:List[Terminal]) extends Actor {

     val father = context.parent
     var nodesAlive:List[Int] = List(id)

     var candSucc:Int = -1
     var candPred:Int = -1
     var status:NodeStatus = new Candidate ()
     var barrier:Int=0

     def receive = {

          // Initialisation
          case Start => {
               self ! Initiate
          }

          case RebootState => {
               println("REBOOT !")
               this.status = new Candidate()
               this.candPred = -1
               this.candSucc = -1
               this.barrier = 0
               this.nodesAlive = List(id)
          }

          case StartWithNodeList (list) => {
               if (list.isEmpty) {
                    this.nodesAlive = this.nodesAlive:::List(id)
               }
               else {
                    this.nodesAlive = list ::: List(id)
               }
               this.nodesAlive = this.nodesAlive.distinct
               this.nodesAlive = this.nodesAlive.sorted
               println(this.nodesAlive)
               // Debut de l'algorithme d'election
               self ! Initiate
          }

          case Initiate => {
               if(this.barrier == 0){
                    status = Candidate()
                    this.candPred = -1
                    this.candSucc = -1
               }
               if(this.nodesAlive.size == 1)
                    getRemoteRingSuccessor(id, this.nodesAlive) ! ALG(this.nodesAlive, id)
               else
                    broadcastSyncStartMsg()
          }

          case ALG (list, init) => {
               println("REC ALG "+init+" "+status+" candSucc="+ candSucc +" candPred=" +candPred)
               status match {
                    case Passive() => {
                         status = Dummy()
                         println("SEND ALG "+ init + " -> succ de "+ id) 
                         getRemoteRingSuccessor(id, list) ! ALG(list, init)
                    }
                    case Candidate() => {
                         this.candPred = init
                         if(id > init){
                              if(this.candSucc == -1){
                                   status = Waiting()
                                   println("SEND AVS "+ id + " -> "+ init)
                                   getRemoteById(init) ! AVS(list, id)
                              } else{
                                   println("SEND AVSRP "+ candPred + " -> "+ candSucc)
                                   getRemoteById(candSucc) ! AVSRSP(list, candPred)
                                   status = Dummy()
                              }
                         }
                         if(init == id){
                              status = Leader()
                              father ! LeaderChanged(id)
                              broadcastLeadChange(id)                    
                         }
                    }

                    case _ =>
               }
          }

          case AVS (list, j) => {
               println("REC AVS("+j+") "+status+" candSucc="+ candSucc +" candPred=" +candPred)
               
               status match {
                    case Candidate() => {
                         if(this.candPred == -1){
                              this.candSucc = j
                         } else{
                              println("SEND AVSRP "+ this.candPred + " -> "+ j)
                              getRemoteById(j) ! AVSRSP(list, this.candPred)
                              status = Dummy()
                         }
                    }
                    case Waiting() => {
                         this.candSucc = j
                         println("Waiting candSucc="+ j)
                    }

                    case _ =>
               }
          }

          case AVSRSP (list, k) => {
               println("REC AVSRP "+k+" "+status+" candSucc="+ candSucc +" candPred=" +candPred)
               status match {
                    case Waiting() =>{
                         if(id == k){
                              status = Leader()
                              this.father ! LeaderChanged(id)
                              broadcastLeadChange(id)
                         }
                         else {
                              candPred = k
                              if(candSucc == -1){
                                   if(k < id){
                                        status = Waiting()
                                        println("SEND AVSRP "+ id + " -> "+ k)
                                        getRemoteById(k) ! AVS(list, id)
                                   }
                              }
                              else{ 
                                   status = Dummy()
                                   println("SEND AVSRP "+ k + " -> "+ candSucc)
                                   getRemoteById(candSucc) ! AVSRSP(list, k)
                              }
                         }
                    }
                    case _ =>
               }
          }

          case Sync(alivesList) => {
               barrier+=1
               this.nodesAlive = alivesList
               println(barrier +" BARRIER|  nodesAliveSize = "+ alivesList.size)
               if(barrier == alivesList.size ){
                    println("SEND ALG "+ id + " -> succ de "+ id) 
                    getRemoteRingSuccessor(id, alivesList) ! ALG(alivesList, id)
               }
          }

     }

     def getRemoteRingSuccessor(i:Int, list: List[Int]) : ActorSelection = {
          getRemoteById(ringSuccessor(i, list))   
     }

     def broadcastLeadChange(i:Int){
          terminaux.foreach({
               case n => {
                    if(n.id != id ){
                         val remote = context.actorSelection("akka.tcp://LeaderSystem" + n.id + "@" + n.ip + ":" + n.port + "/user/Node")
                         remote ! LeaderChanged(i)
                    }
               }
          })
     }

     //TODO unsafe en cas de deconnexion d'un actor
     def getRemoteById(i:Int) : ActorSelection = {
          val n = terminaux.filter({
               case (term) => term.id == i 
          })(0)   // <==  
          context.actorSelection("akka.tcp://LeaderSystem" + n.id + "@" + n.ip + ":" + n.port + "/user/Node/electionActor")
     }

     def ringSuccessor(i:Int, list: List[Int]) : Int = {
          list( (list.indexOf(i)+1) % (list.size))
     }

     def broadcastSyncStartMsg() : Unit= {
          //Thread.sleep(1500)
          terminaux.foreach({
               case n => {
                    if(nodesAlive.contains(n.id) ){
                         val remote = context.actorSelection("akka.tcp://LeaderSystem" + n.id + "@" + n.ip + ":" + n.port + "/user/Node/electionActor")
                         remote ! Sync(this.nodesAlive) 
                         println("SEND SYNC !")
                    }
               }
          })
     }

}
