package com.summerzhou.rpc
//储存worker的信息
class WorkerInfo(val workerId:String,val memory:Int,val cores:Int) {
  //最近一次汇报心跳的时间
  var heartBeatTime : Long = _
}
