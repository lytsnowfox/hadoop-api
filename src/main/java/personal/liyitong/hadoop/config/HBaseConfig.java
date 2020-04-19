package personal.liyitong.hadoop.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

@Configuration
public class HBaseConfig {

    public static final String NAMESPACE = "namespace";

    public static final String TABLE = "table";

    public static final String FAMILY = "family";

    public static final String TABLEMAP = "tableMap";

    public static final String ROWKEY = "rowKey";

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String clientPort;

    @Value("${hbase.zookeeper.znode.parent}")
    private String znodeParent;

    @Bean
    public HbaseTemplate hbaseTemplate(){
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hbase.zookeeper.quorum",zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort",clientPort);
        conf.set("zookeeper.znode.parent",znodeParent);
        return new HbaseTemplate(conf);
    }
}
