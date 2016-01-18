package spark.tan14.cn;

/**
 * Created by xie on 15-10-31.
 */
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class IPFormat {
    /*将ip地址转换成32位的二进制*/
    public static String toBinaryNumber(String ipAddress) {
        String[] octetArray = ipAddress.split("\\.");
        String binaryNumber = "";
        for(String str: octetArray) {
            int octet = Integer.parseInt(str, 10);
            String binaryOctet = Integer.toBinaryString(octet);
            int bolength = binaryOctet.length();
            if(bolength < 8) {
                for (int i = 0; i < 8 - bolength; i++) {
                    binaryOctet = '0' + binaryOctet;			//补前导0
                }
            }
            binaryNumber += (binaryOctet);
        }
        return binaryNumber;
    }

    /*获取network地址部分*/
    public static String getNetworkAddress(String cidrAddress) {
        String[] cidrArray = cidrAddress.split("/");
        String binaryNumber = toBinaryNumber(cidrArray[0]);
        int size = Integer.parseInt(cidrArray[1]);
        return binaryNumber.substring(0, size);
    }

    /**
     * 读取txt文件的内容
     * @param file_name 想要读取的文件对象
     * @return 返回文件内容
     */
    public static TrieTree txt2String(String file_name){
        //String result = "";
        File file = new File(file_name);
        TrieTree t_tree = new TrieTree();
        try{
            BufferedReader br = new BufferedReader(new FileReader(file));//构造一个BufferedReader类来读取文件
            String s = null;
            while((s = br.readLine())!=null){//使用readLine方法，一次读一行
                int index = s.indexOf(",");
                t_tree.insert(getNetworkAddress(s.substring(0,index)),s.substring(index + 1, s.length()));
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        return t_tree;
    }

    public static TrieTree getTrieTree(String file_name) {
        Configuration conf = new Configuration();
//        Path hdfsCoreSitePath = new Path("/home/xie/soft/Spark/hadoop-2.6.0/etc/hadoop/core-site.xml");
        Path hdfsCoreSitePath = new Path("/home/hadoop/hadoop/etc/hadoop/core-site.xml");
        conf.addResource(hdfsCoreSitePath);
        BufferedReader br = null;
        TrieTree t_tree = new TrieTree();
        try {

            FileSystem fileSystem = FileSystem.get(conf);
            FSDataInputStream inputStream = fileSystem.open(new Path(file_name));
            br = new BufferedReader(new InputStreamReader(inputStream));

            String line = null;
            while((line = br.readLine())!=null){//使用readLine方法，一次读一行
                int index = line.indexOf(",");
                t_tree.insert(getNetworkAddress(line.substring(0,index)),line.substring(index + 1, line.length()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return t_tree;
    }

    /*main方法用于测试*/
    public static void main(String[] args) {
        TrieTree t_tree = txt2String("/home/xie/ipdb_yanma.txt");
        long startTime=System.currentTimeMillis();
        System.out.println(t_tree.search(toBinaryNumber("101.xx.xx.xx")));
        long endTime=System.currentTimeMillis(); //获取结束时间
        System.out.println("程序运行时间： "+(endTime-startTime)+"ms");
    }
}
