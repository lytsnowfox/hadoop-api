package personal.liyitong.hadoop.hbase.controller;

import com.alibaba.fastjson.JSONArray;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import personal.liyitong.hadoop.hbase.entity.HBaseParam;
import personal.liyitong.hadoop.hbase.service.HBaseService;

@RestController
@RequestMapping("/hbase")
public class HbaseController {

    @Autowired
    private HBaseService hbaseService;

    @RequestMapping(value = "/getHbaseTables")
    public JSONArray getHbaseTables(String namespace) {
        if (namespace == null) {
            return hbaseService.getAllHbaseInfo();
        }
        return hbaseService.analyseHbaseByNamespace(namespace);
    }

    @RequestMapping(value = "/getFamilyData")
    public JSONArray getFamilyData(@RequestBody HBaseParam param) {
        return hbaseService.getFamilyData(param);
    }

    @RequestMapping(value = "/hBaseDelete", method = RequestMethod.POST)
    public boolean hBaseDelete(@RequestBody HBaseParam param) {
        return hbaseService.deleteData(param);
    }

    @RequestMapping(value = "/hBaseUpdate", method = RequestMethod.POST)
    public boolean hBaseUpdate(@RequestBody HBaseParam param) {
        return hbaseService.updateData(param);
    }

    @RequestMapping(value = "/dropNamespace")
    public int dropNamespace(String namespace) {
        return hbaseService.dropNamespace(namespace);
    }

    @RequestMapping(value = "/dropTable")
    public int dropTable(String tableName) {
        return hbaseService.dropTable(tableName);
    }

    @RequestMapping(value = "/dropFamily")
    public int dropFamily(String tableName, String family) {
        return hbaseService.dropFamily(tableName, family);
    }
}
