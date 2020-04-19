package personal.liyitong.hadoop.hbase.dao;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.stereotype.Repository;
import personal.liyitong.hadoop.config.HBaseConfig;
import personal.liyitong.hadoop.hbase.analysize.HbaseAnalyst;
import personal.liyitong.hadoop.hbase.entity.HBaseParam;
import personal.liyitong.hadoop.hbase.rowkey.RowKeyGenerator;
import personal.liyitong.hadoop.hbase.rowkey.TimestampGenerator;
import personal.liyitong.hadoop.hbase.rowkey.UUIDGenerator;
import personal.liyitong.hadoop.mapreduce.HBaseTableTransfer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Repository(value = "hbaseDaoImpl")
public class HBaseDaoImpl implements HBaseDao {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Override
    public int createNamespace(String... names) {
        int count = 0;
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            for (String name : names) {
                try {
                    admin.createNamespace(NamespaceDescriptor.create(name).build());
                } catch (NamespaceExistException e) {
                    continue;
                }
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return count;
    }

    @Override
    public synchronized int createTable(String tableName, String... families) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTable = new HTableDescriptor(TableName.valueOf(tableName));
                for (String family : families) {
                    hTable.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(hTable);
                return 1;
            }
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public synchronized int alterTable(String tableName, boolean add, String... families) {
        int count = 0;
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor hTable = admin.listTables(tableName)[0];
                try {
                    admin.disableTable(TableName.valueOf(tableName));
                    for (String family : families) {
                        HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                        if (add && !hTable.hasFamily(Bytes.toBytes(family))) {
                            hTable.addFamily(columnDescriptor);
                            count++;
                        } else if (!add && hTable.hasFamily(Bytes.toBytes(family))) {
                            hTable.removeFamily(Bytes.toBytes(family));
                            count++;
                        }
                    }
                    admin.modifyTable(TableName.valueOf(tableName), hTable);
                } finally {
                    admin.enableTable(TableName.valueOf(tableName));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return count;
    }

    @Override
    public synchronized int dropTable(String tableName) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            if (admin.tableExists(TableName.valueOf(tableName))) {
                try {
                    admin.disableTable(TableName.valueOf(tableName));
                } catch (TableNotEnabledException e) {
                }
                admin.deleteTable(TableName.valueOf(tableName));
                return 1;
            }
            return 0;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public synchronized int dropNamespace(String... namespaces) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            for (String namespace : namespaces) {
                admin.deleteNamespace(namespace);
            }
            return 1;
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * 删除的只能是列族，hbase并不关心具体的列
     * @param tableName
     * @param families
     * @return
     */
    @Override
    public int dropFamily(String tableName, String... families) {
        int count = 0;
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            if (admin.isTableAvailable(TableName.valueOf(tableName))) {
                for (String family : families) {
                    admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(family));
                    count++;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        return count;
    }

    @Override
    public int createOrSyncTable(String tableName, String... family) {
        if (createTable(tableName, family) != 1) {
            return alterTable(tableName, true, family);
        }
        return 0;
    }

    @Override
    public int refreshTable(String tableName) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            if (admin.isTableAvailable(TableName.valueOf(tableName))) {
                HTableDescriptor oldTable = admin.listTables(tableName)[0];
                HTableDescriptor newTable = new HTableDescriptor(TableName.valueOf(tableName));
                for (HColumnDescriptor family : oldTable.getFamilies()) {
                    newTable.addFamily(new HColumnDescriptor(family));
                }
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
                admin.createTable(newTable);
                return 1;
            } else {
                return 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public boolean insert(HBaseParam param, RowKeyGenerator generator) {
        hbaseTemplate.execute(param.getTable(), hTable -> {
            JSONArray array = param.getArray();
            for (int i = 0; i < array.size(); i++) {
                JSONObject data = array.getJSONObject(i);
                // 新增或更新
                String id = data.getString(HBaseConfig.ROWKEY);
                if (id == null) {
                    id = generator.generateRowKey();
                }
                Put put = new Put(id.getBytes());
                for (String key : data.keySet()) {
                    if (HBaseConfig.ROWKEY.equals(key)) {
                        continue;
                    }
                    byte[] value = data.getString(key) == null ? null : data.getString(key).getBytes();
                    if (value != null) {
                        put.addColumn(Bytes.toBytes(param.getFamily()), Bytes.toBytes(key), value);
                    }
                }
                hTable.put(put);
            }
            return null;
        });
        return true;
    }

    @Override
    public boolean insert(HBaseParam param) {
        RowKeyGenerator generator = new TimestampGenerator(param.getFamily());
        return insert(param, generator);
    }


    @Override
    public byte[] getColumnValue(HBaseParam param, String qualifier) {
        Cell cell = hbaseTemplate.get(param.getTable(), param.getRowKey(), param.getFamily(),
                qualifier, (result, index) -> {
                    List<Cell> cells = result.listCells();
                    return cells.get(0);
                });
        return cell.getValueArray();
    }

    @Override
    public boolean delete(HBaseParam param) {
        // 如果用户不直接指定参数，那么从hbase数据库进行查询
        if (param.getArray() == null || param.getArray().isEmpty()) {
            param.setArray(query(param));
        }
        for (int i = 0; i < param.getArray().size(); i++) {
            JSONObject obj = param.getArray().getJSONObject(i);
            hbaseTemplate.delete(param.getTable(), obj.getString(HBaseConfig.ROWKEY), param.getFamily());
        }
        return true;
    }

    @Override
    public boolean deleteColumns(HBaseParam param, String... qualifiers) {
        for (String qualifier : qualifiers) {
            hbaseTemplate.delete(param.getTable(), param.getRowKey(), param.getFamily(), qualifier);
        }
        return true;
    }

    @Override
    public HTableDescriptor[] listTable(String regex) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            return admin.listTables(regex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new HTableDescriptor[0];
    }

    @Override
    public TableName[] listTableNamesByNamespace(String namespace) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            return admin.listTableNamesByNamespace(namespace);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new TableName[0];
    }

    @Override
    public JSONArray analyseHbaseTables(HbaseAnalyst analyst) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            analyst.setAdmin(admin);
            analyst.analysizeHbase();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return analyst.getZtreeNodes();
    }

    /**
     * 通用查询
     *
     * @param param
     * @return
     */
    @Override
    public JSONArray query(HBaseParam param) {
        List<JSONObject> list = hbaseTemplate.find(param.getTable(), param.getScan(), (result, rowNum) -> {
            JSONObject obj = new JSONObject();
            // 获取行键
            obj.put(HBaseConfig.ROWKEY, Bytes.toString(result.getRow()));
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                String key = new String(CellUtil.cloneQualifier(cell));
                if (param.getQualifiers() != null && !param.getQualifiers().contains(key)) {
                    continue;
                }
                String value = new String(CellUtil.cloneValue(cell));
                obj.put(key, value);
            }
            return obj;
        });
        JSONArray array = new JSONArray();
        array.addAll(list);
        return array;
    }

    @Override
    public JSONObject getTableData(String tableName) {
        JSONObject data = new JSONObject();
        List<String> families = new ArrayList<>();
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            HTableDescriptor table = admin.listTables(tableName)[0];
            for (HColumnDescriptor family : table.getFamilies()) {
                families.add(family.getNameAsString());
                HBaseParam param = new HBaseParam();
                param.setTable(tableName);
                Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                        new BinaryComparator(Bytes.toBytes(family.getNameAsString())));
                param.addFilter(familyFilter);
                data.put(family.getNameAsString(), query(param));
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return data;
    }

    /**
     * 获取某个列族的数据量
     * @param tableName
     * @param family
     * @return
     */
    @Override
    public Long familySize(String tableName, String family) {
        Long count = Long.valueOf(0);
        try {
            HTable table = new HTable(hbaseTemplate.getConfiguration(), tableName);
            Scan scan = new Scan();
            scan.addFamily(family.getBytes());
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                count += result.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return count;
    }

    /**
     * 将oldTable中的所有数据复制到newTable，适用于新表不存在的情况
     * @param oldTable
     * @param newTable
     * @return
     */
    @Override
    public synchronized Integer copyTable(String oldTable, String newTable) {
        try (Connection connection = ConnectionFactory.createConnection(hbaseTemplate.getConfiguration());
             Admin admin = connection.getAdmin()) {
            if (!admin.isTableAvailable(TableName.valueOf(newTable))) {     // 表不存在的时候方可拷贝
                admin.disableTable(TableName.valueOf(oldTable));
                admin.snapshot("snapshot", TableName.valueOf(oldTable));
                admin.cloneSnapshot("snapshot", TableName.valueOf(newTable));
                admin.deleteSnapshot("snapshot");
                admin.enableTable(TableName.valueOf(oldTable));
                return 1;
            } else {
                return 0;   // 新表已经存在
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * 已存在的表之间的数据传输
     * @param source        源表
     * @param destination   目的表
     * @param transferMap   列族映射关系，key是源表列族，value是输出表的列族
     * @return
     */
    @Override
    public boolean transferHBaseTable(String source, String destination, Map<String, Object> transferMap) {
        Configuration conf = hbaseTemplate.getConfiguration();
        // 向map reduce传参通过configuration实现
        conf.set(HBaseConfig.TABLEMAP, new JSONObject(transferMap).toJSONString());
        try {
            String[] args = new String[0];
            int status = ToolRunner.run(conf, new HBaseTableTransfer(source, destination), args);
            // status 0: 成功，-1: 失败
            return status == 0;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 在已有表中插入列族数据
     * @param table
     * @param family
     * @param array
     */
    @Override
    public void asyncInsertFamily(String table, String family, JSONArray array) {
        asyncInsertFamily(table, family, array, new UUIDGenerator());
    }

    @Override
    public void asyncInsertFamily(String table, String family, JSONArray array, RowKeyGenerator generator) {
        if(alterTable(table, true, family) != -1) {
            HBaseParam param = new HBaseParam();
            param.setTable(table);
            param.setFamily(family);
            param.setArray(array);
            new Thread(()-> insert(param, generator)).start();
        }
    }

    @Override
    public HbaseTemplate getTemplate() {
        return hbaseTemplate;
    }
}
