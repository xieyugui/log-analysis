package spark.tan14.cn

/**
 * Created by xie on 15-7-31.
 */

import com.datastax.spark.connector._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import com.datastax.spark.connector.types._

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
import java.io.{File, IOException}
import spray.json._
import DefaultJsonProtocol._
import java.util.Date
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.partitioner.CassandraPartition
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat


case class PnTraffic(cid: String,ctime: Long, edge: List[Long],edgecode: List[Long],up: List[Long], upcode: List[Long])
case class TopUrl(cid: String,ctime: Long, topurlbytraffic: String,topurlbycount: String)
case class FileArea(cid: String,ctime: Long, filearea :List[Long])
case class IPTopArea(time:Long,iptop :String, iparea :String)


class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}

object analysisLog {
  System.setProperty("spark.cassandra.query.retry.count", "2")  // 数据库尝试连接次数
  //val master = Properties.envOrElse("MASTER","spark://xie:7077")
  //	val sparkHome = Properties.get("SPARK_HOME")
  val master_hostname = "master"
  val cassandra_hostname = "115.xx.xx.xx"
//  val master_hostname = "xie"
//  val cassandra_hostname = "127.0.0.1"
  val conf = new SparkConf(true)
    .setAppName("AnalysisMcLog") // job name
    //.setMaster("yarn-cluster")
    .setMaster("spark://"+master_hostname+":7077")
    .set("spark.cassandra.connection.host", cassandra_hostname)  //数据库连接ＩＰ，集群中任意IP
    .set("spark.cassandra.auth.username", "xxx")      //用户名
    .set("spark.cassandra.auth.password", "xxx")
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")  //序列化

  val ipdb_path = "hdfs://"+master_hostname+":9000/do_not_delete/ipdb_yanma.txt"

  val pattern_str = "\\S+\\s+" +
    "\\S+\\s+" +
    "(\\d+\\.\\d+\\.\\d+\\.\\d+)\\s+" +  //ip
    "\\S+\\s+" + //请求方式 get post
    "\"https?://(\\S+)\"\\/\\S*\\s+" +  // url
    "\\S+\\s+" + //$remote_user
    "\".+\"\\s+" + //文件类型
    "\".+\"\\s+" + //ua
    "\".+\"\\s+" +  //referer
    "(\\S+):(\\S+)\\/\\S+(\\S)\\d{3}\\s+" + //hit or miss or pass    hostname
    "(\\d+)\\s+" +  //code
    "(\\d+)\\s+" +  //send size
    "\\d+\\s+" +  // send body size
    "(\\S*)$"
  val pattern_huitianfu_str = ".*\\[.*\\].*".r.unanchored
  //" HIT:ORIGIN/eacnctczjzhzz01c002
  val pattern_log_str = ".*\"\\s+\\S+:(\\S+)/ .*"

  //.set("spark.ui.port", "4060")
  //  新意URL 统计,已不用
  var interval = 300000L              //时间间隔
  var beginTime:Long = 0l  //开始时间
  var endTime:Long = beginTime + interval//结束时间
  var logCreateTime:String = ""
  val hdfsRootFile = "hdfs://"+master_hostname+":9000/input/"  //默认需要分析的日志名称
  var outFileName = "hdfs://"+master_hostname+":9000/PnLogs/"   // 默认的输出路径
  var topN = 25                           //  top n的数量
  val Country_code = List("86","852","886")
  val qiniuName = "qiniu"
  val huitianfu = "HuiTianFu"
  var nowYearM = ""
  val trie_tree:TrieTree = IPFormat.getTrieTree(ipdb_path)
  val hdfsoperation = new HDFSOperation()   // HDFS 文件操作




  def do_job(xtime :Long): Unit = {
    val timeoperation = new timeOperation()   //时间相关的操作

    var t = System.currentTimeMillis()
    var ctime = new Date().getTime
    if (xtime > 0)
      ctime = xtime
    timeoperation.setBandWidthMilliSec(ctime,interval)  //将当前时间转换成对应带宽所对应的时间戳,  09分对应00分
    val xbeginTime = (timeoperation.getBandWidthMillisSec/1000).toLong  // 获取带宽时间
    val copyBeginTime = xbeginTime;
    println(xbeginTime)
    var begin_time_str = copyBeginTime.toString
    logCreateTime = timeoperation.getYMDHM(xbeginTime*1000)  // 将带宽时间转为yyyy_MM_dd-HH-mm格式
    nowYearM = timeoperation.getYM(xbeginTime*1000)

    val sc = new SparkContext(conf)
    //1.14.128.0/18,中国,上海,上海,方正宽带/电信
//    hdfsoperation.getIPDBTrieTree(ipdb_path,trie_tree);
    val pattern = new Regex(pattern_str)
    //val pattern_huitianfu = new Regex(pattern_huitianfu_str)
    val pattern_log = new Regex(pattern_log_str)
    val pat = Pattern.compile(".*\\[.*\\].*");

    val hdfsfile = hdfsRootFile+logCreateTime+".log"
    println(hdfsfile)

    if( !hdfsoperation.isExists(hdfsfile)) {
      hdfsoperation.putMergeFile(timeoperation.getToOperationDirs(interval),hdfsfile)
    }

    //匹配成功后，返回数据List
    def regexLog(line :String) :List[String] = {
      if (line.isEmpty) List() else {
        val matchList = pattern.findAllIn(line.toLowerCase).matchData.toList
        if (matchList.nonEmpty) matchList(0).subgroups:::List(line) else List()
      }
    }

    val bandwidthTable = "bandwidth"+nowYearM
    val topurlTable = "topurl"+nowYearM
    val fileareaTable = "filearea"+nowYearM
    val areatopipTable = "areatopip"+nowYearM


//    val hdfsfile = "hdfs://xie:9000/input/xie.log"
    val logs = sc.textFile(hdfsfile)
    logs.cache()
    var dataList = logs.map(regexLog(_)).filter(info => info.nonEmpty && info(7).length == 22)
    dataList.cache()//进行cache操作，以便后续操作重用dataList这个变量
    /************************************************************************************************************************/
    //HuiTianFu
    def regexChooseLog(line :String):List[String] = {
      if(line.contains("] \"")) List(huitianfu,line) else {
        val matchList = pattern_log.findAllIn(line.toLowerCase).matchData.toList
        if (matchList.nonEmpty) {
          val logFlag = matchList(0).subgroups(0)
          if (logFlag.isEmpty || logFlag == "-" || logFlag.contains("origin") || logFlag == "pass" || logFlag.contains(".") || logFlag.contains("mozilla"))
            List()
          else List(logFlag, line)
        } else List()
      }
    }

    def detailbandwidth(): Unit = {
      println("Begin detailbandwidth ...")
      val bwMiddle = dataList.map(line => (line(7),commonutil.getBwValue(line)))
      bwMiddle.cache()
      val bwList = bwMiddle.reduceByKey((x ,y) => commonutil.addList(x,y)).
        map(line=> (line._1,copyBeginTime,List(line._2(0),line._2(1),line._2(2),line._2(3),line._2(4),line._2(5)),
        List(line._2(6),line._2(7),line._2(8),line._2(9),line._2(10),line._2(11),line._2(12)),
        List(line._2(13),line._2(14),line._2(15),line._2(16)),
        List(line._2(17),line._2(18),line._2(19),line._2(20),line._2(21),line._2(22),line._2(23))))

      //求all
      val bwListAll = bwMiddle.map(line => ("all",line._2)).reduceByKey((x,y) => commonutil.addList(x,y)).
        map(line=> (line._1,copyBeginTime,List(line._2(0),line._2(1),line._2(2),line._2(3),line._2(4),line._2(5)),
        List(line._2(6),line._2(7),line._2(8),line._2(9),line._2(10),line._2(11),line._2(12)),
        List(line._2(13),line._2(14),line._2(15),line._2(16)),
        List(line._2(17),line._2(18),line._2(19),line._2(20),line._2(21),line._2(22),line._2(23))))

      val mergeInfo =  bwList.union(bwListAll)

      try{
        mergeInfo.saveToCassandra("loganalysis",bandwidthTable ,SomeColumns("cid", "ctime", "edge","edgecode","up", "upcode"))
      } catch{
        case ex: IOException => {
          CassandraConnector(conf).withSessionDo { session =>
            session.execute("CREATE TABLE IF NOT EXISTS loganalysis."+bandwidthTable+" (cid text, ctime bigint, edge list<bigint>, edgecode list<bigint>, up list<bigint>, upcode list<bigint>, PRIMARY KEY (cid,ctime))")
            session.execute("CREATE TABLE IF NOT EXISTS loganalysis."+topurlTable+" (cid text, ctime bigint, topurlbytraffic text, topurlbycount text, PRIMARY KEY (cid,ctime))")
            session.execute("CREATE TABLE IF NOT EXISTS loganalysis."+fileareaTable+" (cid text, ctime bigint, filearea list<bigint>, PRIMARY KEY (cid,ctime))")
            session.execute("CREATE TABLE IF NOT EXISTS loganalysis."+areatopipTable+" (cid text, ctime bigint, iptop text, iparea text, PRIMARY KEY (cid,ctime))")

          }
            //saveAsCassandraTable("loganalysis", bandwidthTable, SomeColumns("cid", "ctime", "edge","edgecode","up", "upcode"))
          mergeInfo.saveToCassandra("loganalysis", bandwidthTable,SomeColumns("cid", "ctime", "edge","edgecode","up", "upcode"))
        }
      }

      bwMiddle.unpersist()

    }

    //分析七牛带宽
    def qiniuBw(): Unit = {
      println("Begin qiniubw ...")
      val qbw = dataList.filter(line => line(3) == qiniuName).map(line => (line(1).split("/")(0),line(6).toLong)).reduceByKey(_+_)
      val fileName = outFileName+qiniuName+"/bandwidth/"+logCreateTime
      try{
        qbw.saveAsTextFile(fileName)
      } catch {
        case ex: IOException => {}
      }

    }


    def topurl(): Unit = {
      println("start topurl")
      val pnUrlCmb = dataList.map(line => ((line(7),line(1).split("\\?")(0)),(line(6).toLong,1l))).
        combineByKey(size => size,(sum:(Long,Long),size:(Long,Long)) =>
        (sum._1+size._1,sum._2+size._2),(sum1:(Long,Long),sum2:(Long,Long)) =>
        (sum1._1+sum2._1,sum1._2+sum2._2)).
        map(info => (info._1._1,(info._1._2,info._2._1,info._2._2))).
        combineByKey(List(_) ,(x:List[(String,Long,Long)],y:(String,Long,Long)) =>
        y::x ,(x:List[(String,Long,Long)] ,y:List[(String,Long,Long)]) => x:::y)

      //pnUrlCmb.collect().foreach(va => {
      //  println(va)
      //((625bd274ffddf5d80a00b1,www.zaccl.com/lp_ad/lp_baidu.js),(638,2)) //combineByKey(size => size.....
      //(625bd274ffddf5d80a00b1,(www.zaccl.com/131_ad/131_baidu_2.js,321,1)) //.map(info => (info._1._1,(info._1._2,info._2._1,info._2._2)))
      //(f64cc132245fbf204b5038,List((cct.picatown.com/fpktgame20120715/res/loadingbar.swf,247,1)))
      //})

      // Get Top N by traffic
      val getTopnByTraffic = pnUrlCmb.map(info => {
        val r=info._2.sortBy(_._2).reverse
        var i = 0
        val urlBuffer = new ListBuffer[(String,(Long,Long))]
        while(i < topN && i < r.length){
          urlBuffer.append((r(i)._1,(r(i)._2,r(i)._3)))
          i=i+1
        }
        (info._1,urlBuffer.toList)
      })

      //Get top N by visit count
      val getTopnByCount = pnUrlCmb.map(info => {
        val r=info._2.sortBy(_._3).reverse
        var i = 0
        val urlBuffer = new ListBuffer[(String,(Long,Long))]
        while(i < topN && i < r.length){
          //	urlBuffer.append((r(i)._1,r(i)._3))
          urlBuffer.append((r(i)._1,(r(i)._2,r(i)._3)))
          i=i+1
        }
        (info._1,urlBuffer.toList)
      })
      //Merge getTopnByTraffic and getTopnByCount by PN
      val mergeInfo = getTopnByTraffic.cogroup(getTopnByCount).map(line=>(line._1,copyBeginTime,line._2._1.toList(0).toJson.toString,line._2._2.toList(0).toJson.toString))
      //RDD[(String, (Iterable[List[(String, Long, Long)]], Iterable[List[(String, Long, Long)]]))]

      try{
        mergeInfo.saveToCassandra("loganalysis",topurlTable ,SomeColumns("cid", "ctime", "topurlbytraffic","topurlbycount"))
      } catch{
        case ex: IOException => {
          //sc.parallelize(Seq(TopUrl("xxx",begin_time_str,"",""))).
          //  saveAsCassandraTable("loganalysis", topurlTable, SomeColumns("cid", "ctime", "topurlbytraffic","topurlbycount"))
          //mergeInfo.saveToCassandra("loganalysis", topurlTable,SomeColumns("cid", "ctime", "topurlbytraffic","topurlbycount"))
        }
      }


    }


    def sizedistribution(): Unit = {
      println("start SizeDistribution")
      val codeList = dataList.filter(line => (line(5).toInt == 200)).map(line => ((line(7),line(1).split("\\?")(0)),line(6).toLong))
      codeList.cache()
      val sizeDisList = codeList.
        combineByKey(size=>size,(sum:Long,v:Long) => (sum),(c1:Long,c2:Long)=>(c1)).
        map(line => (line._1._1,commonutil.sizeCategory(line._2))).reduceByKey((x ,y) => commonutil.addList(x,y))

      //求all
      val sizeDisListAll = codeList.map(line => (("all",line._1._2),line._2)).
        combineByKey(size=>size,(sum:Long,v:Long) => (sum),(c1:Long,c2:Long)=>(c1)).
        map(line => (line._1._1,commonutil.sizeCategory(line._2))).reduceByKey((x ,y) => commonutil.addList(x,y))

      val mergeInfo =  sizeDisList.union(sizeDisListAll).map(line =>(line._1,copyBeginTime,line._2))

      try{
        mergeInfo.saveToCassandra("loganalysis",fileareaTable ,SomeColumns("cid", "ctime", "filearea"))
      } catch{
        case ex: IOException => {
          //sc.parallelize(Seq(FileArea("xxx",begin_time_str,List(0l)))).
          //  saveAsCassandraTable("loganalysis", fileareaTable, SomeColumns("cid", "ctime", "filearea"))
          //mergeInfo.saveToCassandra("loganalysis", fileareaTable,SomeColumns("cid", "ctime", "filearea"))
        }
      }
      codeList.unpersist()

    }

    //ip起始，ip结束， 国家， ISP， area_code
    //18399232 18401279 中国 内蒙古 乌兰察布 联通 15  (地区行政编号取前两位)
    //2 分查找,将IP转为地区Code

    def iptop(): Unit = {
      println("start iptop")


      val PNIPCode = dataList.map(line => ((line(7),line(0)),1l)).
            combineByKey(size => size,(sum:Long,size:Long) =>
            (sum+size),(sum1:Long,sum2:Long) => (sum1+sum2)).
            map(info => (info._1._1,(info._1._2,info._2))).
            combineByKey(List(_) ,(x:List[(String,Long)],y:(String,Long)) =>
            y::x ,(x:List[(String,Long)] ,y:List[(String,Long)]) => x:::y)

      val PnIpIsp = dataList.map(line => ((line(7),trie_tree.search(IPFormat.toBinaryNumber(line(0)))),line(6).toLong)).
                    map(line => ( (line._1._1,(if ((line._1._2 != null) && (line._1._2.split(",")(0) == "中国")) line._1._2 else "其他")),line._2)).
                    reduceByKey(_+_);

      PnIpIsp.cache()
      val AllIsp = PnIpIsp.map(line => (("all",line._1._2),line._2)).reduceByKey(_+_).map(line => (line._1._1,(line._1._2,line._2)));
      val PnIspToList = PnIpIsp.map(line => (line._1._1,(line._1._2,line._2)))


      // Get Top N by count
      val getTopnByTraffic = PNIPCode.map(info => {
        val r=info._2.sortBy(_._2).reverse
        var i = 0
        val ipBuffer = new ListBuffer[(String,Long)]
        while(i < topN && i < r.length){
          ipBuffer.append((r(i)._1,r(i)._2))
          i=i+1
        }
        (info._1,ipBuffer.toList)
      })
      val allTop = sc.parallelize(List(("all",List(("",0L)))));
      val mergeInfo = getTopnByTraffic.union(allTop).cogroup(PnIspToList.union(AllIsp)).
                  map(line=>(line._1,copyBeginTime,line._2._1.toList(0).toJson.toString,line._2._2.toJson.toString))
      try {
        mergeInfo.saveToCassandra("loganalysis", areatopipTable, SomeColumns("cid", "ctime", "iptop", "iparea"))
      }catch {
        case ex: IOException => {}
      }

      PnIpIsp.unpersist()
    }

    /*
  过滤汇添富日志
   */
    def filterHTFLogs(): Unit = {
      println("In fuction of filterHTFLogs!")
      //上上一个时刻,如：现在是 2015-05-24 10:52:00 则取2015-05-24 10:47:00
      //Get the time of one hour ago,format as yyyy_MM_dd-HH
      val fileName = outFileName +huitianfu+"/"+ logCreateTime
      println("Outfile: "+fileName)
      try{
        logs.filter(line => line.contains("] \"")).saveAsTextFile(fileName)
      } catch {
        case ex: IOException => {}
      }
    }

    /*
    过滤其他用户日志
     */
    //rdd.map(_.mkString("\t"))

//    def filterCNameLogs(): Unit = {
//      val otherLog = dataList.map(line=>(line(3),List(line(8)))).filter(line=>
//        !(line._1.isEmpty || line._1 == "-" || line._1.contains("origin") || line._1 == "pass" || line._1.contains(".") || line._1.contains("mozilla"))
//      ).reduceByKey(_:::_).collect().foreach(va => {
//        val (k,v)= va
//        val saveFileName = outFileName + k+"/" + logCreateTime
//        try{
//          sc.parallelize(v).saveAsTextFile(saveFileName)
//        } catch {
//          case ex: IOException => {}
//        }
//      })
//
//    }
  def filterCNameLogs(): Unit = {
    val otherLog = dataList.map(line=>(line(3),line(8))).filter(line=>
      !(line._1.isEmpty || line._1 == "-" || line._1.contains("origin") || line._1 == "pass" || line._1.contains(".") || line._1.contains("mozilla"))
    ).partitionBy(new HashPartitioner(8))
      .saveAsHadoopFile(outFileName+logCreateTime, classOf[String], classOf[String],classOf[RDDMultipleTextOutputFormat])

  }

    /************************************************************************************************************************/



    filterHTFLogs()

    filterCNameLogs()
    logs.unpersist()
    dataList = dataList.map(line => line.slice(0, line.size - 1))
//    详细带宽数据
    detailbandwidth()
    qiniuBw()
    topurl()
    sizedistribution()
    iptop()
    dataList.unpersist();
    sc.stop()
  }




  def main(args:Array[String]): Unit ={


    var i = 0
    while(i < args.length) {
      println(args(i))
      args(i).toLowerCase match {
        case "-i" | "--interval" => this.interval = args(i + 1).toLong
        case "-bt" |"--begintime" => this.beginTime = args(i + 1).toLong //+ this.interval
        case "-et" | "--endtime" => this.endTime = args(i + 1).toLong //+ this.interval
        case _ =>
      }
      i = i + 1
    }
    var ctime:Long=beginTime
    while(ctime < endTime) {
      do_job(ctime)
      ctime = ctime + interval
    }



  }

}
