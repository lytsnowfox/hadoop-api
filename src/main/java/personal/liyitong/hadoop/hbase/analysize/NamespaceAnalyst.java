package personal.liyitong.hadoop.hbase.analysize;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import personal.liyitong.hadoop.config.HBaseConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NamespaceAnalyst extends HbaseAnalyst {

    private List<String> ignores;

    private boolean reverse;

    public NamespaceAnalyst(List<String> ignores, boolean reverse) {
        super();
        this.ignores = ignores;
        this.reverse = reverse;
    }

    public NamespaceAnalyst() {
        super();
        ignores = new ArrayList<>();
        this.reverse = false;
    }

    @Override
    public JSONArray analysizeHbase() {
        try {
            NamespaceDescriptor[] descriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor descriptor: descriptors) {
                if (ignores.contains(descriptor.getName()) == reverse) {
                    this.namespaces.put(id, descriptor);
                    JSONObject root = new JSONObject();
                    root.put("id", id++);
                    root.put("name", descriptor.getName());
                    root.put("type", HBaseConfig.NAMESPACE);
                    ztreeNodes.add(root);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return super.analysizeHbase();
    }
}
