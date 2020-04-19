package personal.liyitong.hadoop.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;

/**
 * hadoop工具类
 */
@Component
public class HdfsConfig {

    @Value("${hdfs.path}")
    private String path;

    @Value("${hdfs.username}")
    private String username;

    private Configuration config;
    /**
     * 获取HDFS配置信息
     */
    private Configuration getConfig() {
        if (config == null) {
            config = new Configuration();
            config.set("fs.defaultFS", path);
        }
        return config;
    }

    /**
     * 获取HDFS文件系统对象
     */
    public FileSystem getFileSystem() {
        //客户端去操作HDFS是有一个用户身份的，默认情况下HDFS客户端API会从jvm中获取一个参数作为自己的用户身份 HADOOP_USER_NAME=hadoop
        //FileSystem hdfs = FileSystem.get(getHdfsConfig());//默认获取
        //也可以在构造客户端fs对象时，通过参数传递
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(path), getConfig(), username);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fileSystem;
    }
}
