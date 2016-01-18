package spark.tan14.cn

/**
 * Created by macor on 15-2-11.
 */
import java.io._

import org.apache.hadoop.conf.Configuration
import java.io._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils

/**
 * Created by root on 1/11/15.
 */
class HDFSOperation {
	private val conf = new Configuration()
//	private val hdfsCoreSitePath = new Path("/home/xie/soft/Spark/hadoop-2.6.0/etc/hadoop/core-site.xml")
	private val hdfsCoreSitePath = new Path("/home/hadoop/hadoop/etc/hadoop/core-site.xml")
	conf.addResource(hdfsCoreSitePath)
	private val fileSystem = FileSystem.get(conf)



	//println(conf.get("fs.default.name"))

	def saveFile(localfile: String): Unit = {
		val localFile = new File(localfile)

		if (localFile.isFile){
			try{
				val out = fileSystem.create(new Path("/input/"+localFile.getName))
				val hadoopDir = new Path("/input/")
				val in = new BufferedInputStream(new FileInputStream(localFile))
				var b = new Array[Byte](1024)
				var numBytes = in.read(b)
				while (numBytes > 0) {
					out.write(b, 0, numBytes)
					numBytes = in.read(b)
				}
				in.close()
				out.close()
				println("上传成功")
				//	val hadfiles:FileStatus = fileSystem.getFileStatus(hadoopDir)
				//for (fs <- hadfiles) {
				//println(hadfiles.getName)
				//}
			}catch{
				case ex:FileNotFoundException => println("未找到文件")
				case ex:IOException =>  println("删除文件出错")
			}
		}
	}

	//将本地文件爱你上传到HDFS ，然后删除本地文件
	def moveLocalFileToHDFS(localfile: String): String = {
		val localFile = new File(localfile)
		val hdfsFileName = "/input/"+localFile.getName
		if (localFile.exists){
			try{
				val localPath = new Path(localfile)
				println(localFile.getAbsolutePath)
				val HDFSPath = new Path(hdfsFileName)
				//fileSystem.copyFromLocalFile(True,localPath,HDFSPath)
				fileSystem.moveFromLocalFile(localPath,HDFSPath)
				println("上传成功")
				//val hadoopDir = new Path("/input/")
				//val hadfiles = fileSystem.getFileStatus(hadoopDir)
				//val hadfiles = new FileStatus()
				//println(hadfiles.getPath)

			}catch{
				case ex:FileNotFoundException => println("未找到文件")
				case ex:IOException =>  println("删除文件出错")
			}
		}
		hdfsFileName
	}

	def putMergeFile(LocalDir:List[String], fsFile:String)
	{
		//val  conf:Configuration = new Configuration()
		val fs:FileSystem = FileSystem.get(conf)       //fs是HDFS文件系统
		val local:FileSystem = FileSystem.getLocal(conf)   //本地文件系统


		val HDFSFile:Path = new Path(fsFile)
		val out:FSDataOutputStream = fs.create(HDFSFile)       //在HDFS上创建输出文件

		for(localList <- LocalDir){
			val localDir:Path = new Path(localList)   //生成输入路径
			val status = local.listStatus(localDir) // local.listStatus(localDir)  //得到输入目录

			for(st <- status)
			{
				val temp:Path = st.getPath()
				if(st.getPath.getName.endsWith(".log")) {
					val in: FSDataInputStream = local.open(temp)
					IOUtils.copyBytes(in, out, 8192, false) //读取in流中的内容放入out
					in.close() //完成后，关闭当前文件输入流
				}
			}
		}
		out.close()
	}


	//删除1小时前hdfs://master:9000/input/目录下的文件
	def getFileStatus(dir:String,now:Long): Unit =
	{
		val path = new Path(dir)
		for( filestatus <- fileSystem.listStatus(path)){
			//println(filestatus.getPath.getParent.toString+"/"+filestatus.getPath.getName+": "+filestatus.getModificationTime)
			val d = now - filestatus.getModificationTime
			val fn = filestatus.getPath.getName
			println(fn+" : "+now+" - "+filestatus.getModificationTime+" = "+d)
			if(d > 3600000){
				if (fileSystem.deleteOnExit(filestatus.getPath)){
					println( fn + " is deleted!")
				}
			}
		}
		println("Delete over")
	}

	//删除timeout 毫秒前的文件，hdfs://master:9000/input/目录下的文件
	def getFileStatus(dir:String,now:Long,timeout:Long): Unit = {
		//val path = new Path(dir)
		for( filestatus <- fileSystem.listStatus(new Path(dir))){
			//println(filestatus.getPath.getParent.toString+"/"+filestatus.getPath.getName+": "+filestatus.getModificationTime)
			val d = now - filestatus.getModificationTime
			val fn = filestatus.getPath.getName
			if(d > timeout){
				println(fn+" : "+now+" - "+filestatus.getModificationTime+" = "+d)
				if (fileSystem.delete(filestatus.getPath,true)){
					println( fn + " is deleted!")
				}
			}
		}
		println("Delete over")
	}

	def removeFile(filename: String): Boolean = {
		//val path = new Path(filename)
		////val d = fileSystem.isDirectory(path)
		fileSystem.delete(new Path(filename), true)
	}

	def isExists(filename: String): Boolean = {
		//val path = new Path(filename)
		////val d = fileSystem.isDirectory(path)
		fileSystem.exists(new Path(filename))
		//fileSystem.exists(filename)
	}

	def getFile(filename: String): InputStream = {
		val path = new Path(filename)
		fileSystem.open(path)
	}

	def createFolder(folderPath: String): Unit = {
		val path = new Path(folderPath)
		if (!fileSystem.exists(path)) {
			fileSystem.mkdirs(path)
		}
	}
}
