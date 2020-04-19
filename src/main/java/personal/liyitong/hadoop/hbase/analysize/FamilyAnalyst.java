package personal.liyitong.hadoop.hbase.analysize;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import personal.liyitong.hadoop.config.HBaseConfig;

import java.io.IOException;
import java.util.HashMap;

public class FamilyAnalyst extends HbaseAnalyst {

    public FamilyAnalyst() {
        super();
    }

    public FamilyAnalyst(Integer id, String table) {
        this.tableNames = new HashMap<>();
        this.tableNames.put(id, TableName.valueOf(table));
        this.ztreeNodes = new JSONArray();
        JSONObject root = new JSONObject();
        root.put("id", id++);
        root.put("name", table);
        root.put("type", HBaseConfig.TABLE);
        this.ztreeNodes.add(root);
        this.id = id;
    }

    @Override
    public JSONArray analysizeHbase() {
        try {
            for (Integer key : this.tableNames.keySet()) {
                HTableDescriptor[] tables = admin.listTables(tableNames.get(key).getNameAsString());
                if (tables != null && tables.length > 0) {
                    for (HColumnDescriptor family : tables[0].getFamilies()) {
                        JSONObject column = new JSONObject();
                        column.put("id", id++);
                        column.put("parentId", key);
                        column.put("name", family.getNameAsString());
                        column.put("type", HBaseConfig.FAMILY);
                        this.ztreeNodes.add(column);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return super.analysizeHbase();
    }
}
