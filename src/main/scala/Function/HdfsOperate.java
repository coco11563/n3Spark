package Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.net.URI;

public class HdfsOperate implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(HdfsOperate.class);
    private static Configuration conf = new Configuration();
    private static BufferedWriter writer = null;

    //在hdfs的目标位置新建一个文件，得到一个输出流
    public static void openHdfsFile(String path) throws Exception {
        FileSystem fs = FileSystem.get(URI.create(path),conf);
        writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))));
        if(null!=writer){
            logger.info("[HdfsOperate]>> initialize writer succeed!");
        }
    }

    //往hdfs文件中写入数据
    public static void writeString(String line) {
        try {
            writer.write(line + "\n");
        }catch(Exception e){
            logger.error("[HdfsOperate]>> writer a line error:"  ,  e);
        }
    }

    //关闭hdfs输出流
    public static void closeHdfsFile() {
        try {
            if (null != writer) {
                writer.close();
                logger.info("[HdfsOperate]>> closeHdfsFile close writer succeed!");
            }
            else{
                logger.error("[HdfsOperate]>> closeHdfsFile writer is null");
            }
        }catch(Exception e){
            logger.error("[HdfsOperate]>> closeHdfsFile close hdfs error:" + e);
        }
    }

}