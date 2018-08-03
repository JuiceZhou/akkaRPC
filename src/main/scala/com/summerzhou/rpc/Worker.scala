package com.summerzhou.rpc

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.{Actor, ActorLogging, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Worker(val masterHost:String,val masterPost:String,val memory:Int,val cores:Int) extends Actor with ActorLogging{
  var master : ActorSelection = _
  val workerId : String = UUID.randomUUID().toString
  val HEARTBEAT_INTERVAL = 10000
  //在接收方法调用前调用preStart方法来创建和Master的连接
  override def preStart(): Unit = {
    //1.和Master建立连接 2.获得master的引用
    //ActorContext用来建立连接，参数是Master的路径，返回一个masterSelection，表示master的代理
    master = context.actorSelection(s"akka.tcp://actorSystem@$masterHost:$masterPost/user/master1")
    //向master发送注册信息
    master ! RegisterWorker(workerId,memory,cores)
    log.info("worker发送注册消息...")
  }
  override def receive: Receive = {
    //接收master注册成功的回复，每10s汇报一次心跳信息
    case RegisterMaster(url) =>{
      println(url)
      //导入隐藏类型包
      import context.dispatcher
      //akka自带的定时器，因为没有master的actorRef对象，所以先发送给自己，然后使用actorSelection发送
      context.system.scheduler.schedule(0 millis,HEARTBEAT_INTERVAL millis,self,SendHeartBeat(workerId))
    }
    case SendHeartBeat(workerId) => {
      log.info("向master发送心跳信息...")
      //将心跳发送至Master
      master ! HeartBeat(workerId)
    }

  }
}
object Worker{
  def main(args: Array[String]): Unit = {
    //建立ActorSystem，单例
    //args提供参数，表示ActorSystem所在机器的port和ip地址
    val host = args(0)
    val port = args(1)
    val masterHost = args(2)
    val masterPort = args(3)
    val memory = args(4).toInt
    val cores = args(5).toInt
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
    val masterActor = actorSystem.actorOf(Props(new Worker(masterHost,masterPort,memory,cores)),"worker1")
    //进程等待
    actorSystem.awaitTermination()
  }
}

