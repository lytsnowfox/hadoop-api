package personal.liyitong.hadoop.hbase.analysize;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;

import java.util.HashMap;
import java.util.Map;

/**
 * Hbase元数据解析，职责链模式
 */
public abstract class HbaseAnalyst {

    protected Integer id;

    protected JSONArray ztreeNodes;

    protected Admin admin;

    Map<Integer, NamespaceDescriptor> namespaces;

    Map<Integer, TableName> tableNames;

    private HbaseAnalyst nextAnalyst;

    protected HbaseAnalyst() {
        this.id = 0;
        this.ztreeNodes = new JSONArray();
        this.namespaces = new HashMap<>();
        this.tableNames = new HashMap<>();
    }

    public JSONArray analysizeHbase(){
        if (this.nextAnalyst != null) {
            this.nextAnalyst.setId(this.id);
            this.nextAnalyst.setAdmin(this.admin);
            this.nextAnalyst.setNamespaces(this.namespaces);
            this.nextAnalyst.setTableNames(this.tableNames);
            this.nextAnalyst.setZtreeNodes(this.ztreeNodes);
            return nextAnalyst.analysizeHbase();
        } else {
            return this.ztreeNodes;
        }
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public JSONArray getZtreeNodes() {
        return ztreeNodes;
    }

    public void setZtreeNodes(JSONArray ztreeNodes) {
        this.ztreeNodes = ztreeNodes;
    }

    public Admin getAdmin() {
        return admin;
    }

    public void setAdmin(Admin admin) {
        this.admin = admin;
    }

    public HbaseAnalyst setNextAnalyst(HbaseAnalyst nextAnalyst) {
        this.nextAnalyst = nextAnalyst;
        return this.nextAnalyst;
    }

    public Map<Integer, NamespaceDescriptor> getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(Map<Integer, NamespaceDescriptor> namespaces) {
        this.namespaces = namespaces;
    }

    public Map<Integer, TableName> getTableNames() {
        return tableNames;
    }

    public void setTableNames(Map<Integer, TableName> tableNames) {
        this.tableNames = tableNames;
    }
}
