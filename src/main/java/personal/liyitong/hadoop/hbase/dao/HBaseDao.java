package personal.liyitong.hadoop.hbase.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import personal.liyitong.hadoop.hbase.analysize.HbaseAnalyst;
import personal.liyitong.hadoop.hbase.entity.HBaseParam;
import personal.liyitong.hadoop.hbase.rowkey.RowKeyGenerator;

import java.util.Map;

public interface HBaseDao {

    int createNamespace(String... names);

    int createTable(String tableName, String... families);

    int alterTable(String tableName, boolean add, String... families);

    int dropTable(String tableName);

    int dropNamespace(String... namespaces);

    int refreshTable(String tableName);

    int dropFamily(String tableName, String... family);

    int createOrSyncTable(String tableName, String... family);

    boolean insert(HBaseParam param);

    boolean insert(HBaseParam param, RowKeyGenerator generator);

    byte[] getColumnValue(HBaseParam param, String qualifier);

    boolean delete(HBaseParam param);

    boolean deleteColumns(HBaseParam param, String... qualifiers);

    JSONObject getTableData(String tableName);

    JSONArray query(HBaseParam param);

    Long familySize(String tableName, String family);

    HTableDescriptor[] listTable(String regex);

    JSONArray analyseHbaseTables(HbaseAnalyst analyst);

    TableName[] listTableNamesByNamespace(String namespace);

    Integer copyTable(String oldTable, String newTable);

    boolean transferHBaseTable(String source, String destination, Map<String, Object> transferMap);

    void asyncInsertFamily(String table, String family, JSONArray array);

    void asyncInsertFamily(String table, String family, JSONArray array, RowKeyGenerator generator);

    HbaseTemplate getTemplate();
}
