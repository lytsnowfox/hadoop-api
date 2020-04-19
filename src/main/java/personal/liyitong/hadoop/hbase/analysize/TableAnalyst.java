package personal.liyitong.hadoop.hbase.analysize;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import personal.liyitong.hadoop.config.HBaseConfig;

import java.io.IOException;
import java.util.HashMap;

public class TableAnalyst extends HbaseAnalyst {

    public TableAnalyst() {
        super();
    }

    public TableAnalyst(Integer id, String namespace) {
        this.namespaces = new HashMap<>();
        this.namespaces.put(id, NamespaceDescriptor.create(namespace).build());
        this.tableNames = new HashMap<>();
        this.ztreeNodes = new JSONArray();
        JSONObject root = new JSONObject();
        root.put("id", id++);
        root.put("name", namespace);
        root.put("type", "namespace");
        this.ztreeNodes.add(root);
        this.id = id;
    }

    @Override
    public JSONArray analysizeHbase() {
        try {
            for (Integer key : namespaces.keySet()) {
                NamespaceDescriptor namespace = namespaces.get(key);
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace.getName());
                for (TableName tableName : tableNames) {
                    this.tableNames.put(id, tableName);
                    JSONObject node = new JSONObject();
                    node.put("id", id++);
                    node.put("parentId", key);
                    node.put("name", tableName.getNameAsString());
                    node.put("type", HBaseConfig.TABLE);
                    ztreeNodes.add(node);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return super.analysizeHbase();
    }
}
