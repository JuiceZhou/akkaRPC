package com.summerzhou.rpc

import akka.actor._
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.collection.mutable

class Master(val host:String,val port:String,val masterName:String) extends Actor with ActorLogging {
  log.info("MasterStarting......")
  //用来储存注册过的worker的信息 (workerId,WorkerInfo)
  val IdToWorkerMap = new mutable.HashMap[String,WorkerInfo]()
  //用来保存注册Worker，方便后续排序处理
  val workerSet = new mutable.HashSet[WorkerInfo]()
  var CHECK_INTERVAL = 15000
  //在消息接收之前就开启定时任务，按时检查worker的状态
  override def preStart(): Unit = {
    log.info("开启心跳检查....")
    import context.dispatcher
    context.system.scheduler.schedule(0 millis,CHECK_INTERVAL millis,self,CheckWorker)
  }
  override def receive: Receive = {
    //接收worker的注册消息，需要将注册的worker保存在内存中
    case RegisterWorker(workId,memory,cores) =>{
      //注册未注册过的worker
      if(!IdToWorkerMap.contains(workId)){
        val workerInfo = new WorkerInfo(workId,memory,cores)
        //加入map中
        IdToWorkerMap += (workId->workerInfo)
        //加入set中
        workerSet += workerInfo
        log.info("注册成功...")
        //发送消息给Worker
        sender ! RegisterMaster(s"akka.tcp://actorSystem@$host:$port/user/$masterName")
      }
    }
      //心跳信息处理
    case HeartBeat(workerId) => {
      val currentTime = System.currentTimeMillis()
      //从map中获取对应的worker，添加心跳信息
      val workerInfo = IdToWorkerMap(workerId)
      workerInfo.heartBeatTime = currentTime
      log.info("心跳时间记录成功...")
    }
      //定时检查worker
    case CheckWorker =>{
      //获取心跳与当前时间相差CHECK_INTERVAL以上的worker
      val diedWorkers = workerSet.filter(x => System.currentTimeMillis() - x.heartBeatTime > CHECK_INTERVAL)
      //将出问题的worker从集合中剔除
      for(diedWorker <- diedWorkers){
        val workerId = diedWorker.workerId
        IdToWorkerMap -= workerId
        workerSet -= diedWorker
      }
      log.info("当前有效的worker数为:"+workerSet.size)
    }
  }
}
object Master{
  def main(args: Array[String]): Unit = {
    //args提供参数，表示ActorSystem所在机器的port和ip地址
    val host = args(0)
    val port = args(1)
    //stripMargin表示之前的字符串通过|来分割 ，分为三行的String
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    //加载构造文件
    val config = ConfigFactory.parseString(configStr)
    //创建ActorSystem，用来创建和停止Actor，单例
    val actorSystem = ActorSystem("actorSystem",config)
    //创建Master Actor的actorRef
    val masterName = "master1"
    val masterActor = actorSystem.actorOf(Props(new Master(host,port,masterName)),masterName)
    //actorRef发送QuoteRequest message
    masterActor ! "hello"
    //进程等待
    actorSystem.awaitTermination()
  }
}
