package spark.tan14.cn

import scala.collection.mutable.ListBuffer

/**
 * Created by xie on 15-8-21.
 */
object commonutil {

  val sizeC = List(100l,1024l,3072l,5120l,10240l,32768l,65536l,131072l,262144l,524288l,1048576l,2097152l,10485760l,52428800l,104857600l)

  def addList(a:List[Long],b:List[Long]):List[Long]={
    val lbuffer = new ListBuffer[Long]
    val length = a.length
    for(i <- 0 until length){
      lbuffer.append(a(i) + b(i))
    }
    lbuffer.toList
  }


  //(edgeTraffic,edgeCount,edgePassTraffic,edgePassCount,edgeBackTraffic,edgeBackcount,edge5xx,edge502,edge503,edge504,
  // edge4xx,edge403,edge404
  // upTraffic,upCount,upBackTraffic,upBackcount,up5xx,up502,up503,up504,up4xx,up403,up404)
  def getBwValue(line :List[String]) :List[Long] = {
    var edgeT = 0l
    var edgeTC = 0l
    var edgeP = 0l
    var edgePC = 0l
    var edgeB = 0l
    var edgeBC = 0l

    var upT = 0l
    var upTC = 0l
    var upB = 0l
    var upBC = 0l

    var c5xx = 0l
    var c502 = 0l
    var c503 = 0l
    var c504 = 0l
    var c4xx = 0l
    var c403 = 0l
    var c404 = 0l

    if (line(4) == "m") {
      upT = line(6).toLong
      upTC = 1l
      if (line(2) == "miss") {
        upB = upT
        upBC = 1l
      }
    } else {
      edgeT = line(6).toLong
      edgeTC = 1l
      line(2) match {
        case "pass" => { edgeP = edgeT;edgePC = 1l}
        case "miss" => { edgeB = edgeT;edgeBC = 1l}
        case _ => {}
      }
    }
    line(5)(0).toString match {
      case "5" => {
        c5xx = 1l;
        line(5) match {
          case "502" => c502 = 1l
          case "503" => c503 = 1l
          case "504" => c504 = 1l
          case _ => {}
        }
      }
      case "4" => {
        c4xx = 1l;
        line(5) match {
          case "403" => c403 = 1l
          case "404" => c404 = 1l
          case _ => {}
        }
      }
      case _ => {}
    }
    if (edgeT > 0) List(edgeT,edgeTC,edgeP,edgePC,edgeB,edgeBC,c5xx,c502,c503,c504,c4xx,c403,c404,upT,upTC,upB,upBC,0l,0l,0l,0l,0l,0l,0l)
    else List(edgeT,edgeTC,edgeP,edgePC,edgeB,edgeBC,0l,0l,0l,0l,0l,0l,0l,upT,upTC,upB,upBC,c5xx,c502,c503,c504,c4xx,c403,c404)
  }


  //内容分类
  /*
  <= 100B       100 B
  <= 1KB        1024 B
  <= 3KB        3072 B
  <= 5KB        5120 B
  <= 10kB       10240 B
  <= 32KB       32768 B
  <= 64KB       65536 B
  <= 128KB      131072 B
  <= 256KB      262144 B
  <= 512KB      524288 B
  <= 1M         1048576 B
  <= 2M         2097152 B
  <= 10M        10485760 B
  <= 50M        52428800 B
  <= 100M       104857600 B
  other > 100M
  */
  def sizeCategory(sendsize :Long): List[Long] = {
    val lbuffer = ListBuffer(0l,0l,0l,0l,0l,0l,0l,0l,0l,0l,0l,0l,0l,0l,0l,0l)
    val slength = sizeC.length
    var done = false
    for(i <- 0 until slength ; if !done){
      if (sendsize <= sizeC(i)) {
        lbuffer(i) = 1l
        done = true
      }
    }
    if(!done) {
      lbuffer(slength) = 1l
    }
    lbuffer.toList
  }

  /*
  将IP 转为所在段的第一个，即将IP 的最后一个数字变为1
 */
  def ipReplace(ip:String): Unit ={
    ip.substring(0,ip.lastIndexOf("."))+".1"
  }

  /*
  将Ip 字符串转为长整数   #将IP 转为所在段的第一个，即将IP 的最后一个数字变为1
 */
  def ip2long(ip:String):Long={
    val ips = ip.split("[.]")
    16777216L*ips(0).toLong + 65536L*ips(1).toLong + 256*ips(2).toLong + 1l
  }

}
