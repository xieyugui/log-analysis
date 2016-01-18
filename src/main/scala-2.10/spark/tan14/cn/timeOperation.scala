package spark.tan14.cn

/**
 * Created by macor on 15-2-11.
 */
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer

class timeOperation {
	//System.currentTimeMillis() 效率会更高
	private val preDir = "/data/cdnlog/"
	//当前时间的毫秒数
	private val currentMillisSec = new Date().getTime
	//val remainderSec:Int = (sec/300).toInt
	//最近分钟数是5的倍数的毫秒数
	private var latestMilliSec300 = currentMillisSec - currentMillisSec%300000
	//带宽所属于的毫秒数
	private var bandWidthMilliSec = latestMilliSec300 - 300000
	//最近分钟数是5的倍数的毫秒数
	private var lastLogMillisSec = latestMilliSec300 - 60000

	//val outfile = preDir +"tmp/"+countMilliSec+ ".log"
	//日志所在目录的所在小时数
	//private val hourDir = preDir+formatterHour.format(firstLogMilliSec).toString
	//private val minDir = List(preDir+new SimpleDateFormat("yyyy_MM_dd/HH").format(firstLogMillisSec).toString)

	//获取带宽所属的毫秒数
	def getBandWidthMillisSec=this.bandWidthMilliSec

	def getNowMilliSec():Long={
		new Date().getTime
	}

	//获取当前的毫秒数
	def getCurrentMilliSec = currentMillisSec

	def setLastLogMillisSec(millisec:Long) {
		lastLogMillisSec = millisec - currentMillisSec%300000 - 60000
	}

	def setBandWidthMilliSec(millisec:Long) {
		latestMilliSec300 = millisec - millisec%300000
		bandWidthMilliSec = latestMilliSec300 - 300000
		//lastLogMillisSec = bandWidthMilliSec - 60000   //被坑了
		lastLogMillisSec = latestMilliSec300 - 60000
	}

	/*
		Set bandwidth time by user define interval
	 */
	def setBandWidthMilliSec(millisec:Long,interval:Long) {
		latestMilliSec300 = millisec - millisec%interval
		bandWidthMilliSec = latestMilliSec300 - interval
		//lastLogMillisSec = bandWidthMilliSec - 60000   //被坑了
		lastLogMillisSec = latestMilliSec300 - 60000
	}

	//获得要操作的日志所对应的毫秒数列表
	def getToOperationMillisSec =List(
		lastLogMillisSec - 60000*4,
		lastLogMillisSec - 60000*3,
		lastLogMillisSec - 60000*2,
		lastLogMillisSec - 60000*1,
		lastLogMillisSec)

	//获取需要操作的文件目录,默认为5分钟
	def getToOperationDirs = List(
		preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*4).toString,
		preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*3).toString,
		preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*2).toString,
		preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*1).toString,
		preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec).toString
	)

	/*
	获取2015_05_14/12/20格式的日期格式
	 */
	def getYMDHMDir(millissec:Long)=new SimpleDateFormat("yyyy_MM_dd/HH/mm").format(millissec).toString
	def getYMDHDir(millissec:Long)=new SimpleDateFormat("yyyy_MM_dd/HH").format(millissec).toString
	def getYMDHM(millissec:Long)=new SimpleDateFormat("yyyy_MM_dd-HH-mm").format(millissec).toString
	def getYMDH(millissec:Long)=new SimpleDateFormat("yyyy_MM_dd-HH").format(millissec).toString
	def getYM(millissec:Long) = new SimpleDateFormat("yyyyMM").format(millissec).toString

	//获取需要操作的文件目录
	def getToOperationDirs(interval:Long): List[String]={
		val dirBuffer = new ListBuffer[String]
		val count = (interval/60000).toInt -1
		//var fn:String = ""
		for(i <- 0 to count)
		{
			//fn = preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*i).toString
			//dirBuffer.append(fn)
			dirBuffer.append(preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*(count - i)).toString)
			println(preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*(count - i)).toString)
		}
		dirBuffer.toList
		//preDir + new SimpleDateFormat("yyyy_MM_dd/HH/mm/").format(lastLogMillisSec - 60000*4).toString
	}

	def millisSecToString(millis:Long):String={
		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(millis).toString
	}
}
