package personal.liyitong.hadoop.hbase.service;

import com.alibaba.fastjson.JSONArray;
import personal.liyitong.hadoop.hbase.entity.HBaseParam;

public interface HBaseService {

    JSONArray analyseHbaseByNamespace(String namespace);

    JSONArray getAllHbaseInfo();

    JSONArray analyseTable(String tableName);

    JSONArray getFamilyData(HBaseParam param);

    boolean deleteData(HBaseParam param);

    boolean updateData(HBaseParam param);

    int createTable(String tableName, String... families);

    int dropTable(String tableName);

    int dropFamily(String tableName, String family);

    int dropNamespace(String... namespaces);
}
