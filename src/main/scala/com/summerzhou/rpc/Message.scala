package com.summerzhou.rpc
//用来实现序列化
trait Message extends Serializable{

}


//worker->master，用来注册worker，需要提供worker对应的id，分配的内存和cpu核数
case class RegisterWorker(workerId:String,memory:Int,cores:Int) extends Message
//worker -> self 心跳信息
case class SendHeartBeat(workerId:String)
//worker -> master 心跳信息
case class HeartBeat(workerId:String) extends Message

//master -> worker 发送master的url，方便worker汇报心跳信息
case class RegisterMaster(url:String) extends Message
//master -> self 定时检查worker状态
case class CheckWorker()