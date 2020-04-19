package personal.liyitong.hadoop.hbase.service;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import personal.liyitong.hadoop.hbase.analysize.FamilyAnalyst;
import personal.liyitong.hadoop.hbase.analysize.HbaseAnalyst;
import personal.liyitong.hadoop.hbase.analysize.NamespaceAnalyst;
import personal.liyitong.hadoop.hbase.analysize.TableAnalyst;
import personal.liyitong.hadoop.hbase.dao.HBaseDao;
import personal.liyitong.hadoop.hbase.entity.HBaseParam;

@Service
public class HBaseServiceImpl implements HBaseService {

    @Autowired
    private HBaseDao hbaseDao;

    @Override
    public JSONArray analyseHbaseByNamespace(String namespace) {
        HbaseAnalyst analyst = new TableAnalyst(0, namespace);
        analyst.setNextAnalyst(new FamilyAnalyst());
        return hbaseDao.analyseHbaseTables(analyst);
    }

    @Override
    public JSONArray getAllHbaseInfo() {
        HbaseAnalyst analyst = new NamespaceAnalyst();
        analyst.setNextAnalyst(new TableAnalyst()).setNextAnalyst(new FamilyAnalyst());
        return hbaseDao.analyseHbaseTables(analyst);
    }

    @Override
    public JSONArray analyseTable(String tableName) {
        HbaseAnalyst analyst = new FamilyAnalyst(0, tableName);
        return hbaseDao.analyseHbaseTables(analyst);
    }

    @Override
    public JSONArray getFamilyData(HBaseParam param) {
        Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(param.getFamily())));
        param.addFilter(familyFilter);
        return hbaseDao.query(param);
    }

    @Override
    public boolean deleteData(HBaseParam param) {
        return hbaseDao.delete(param);
    }

    @Override
    public boolean updateData(HBaseParam param) {
        return hbaseDao.insert(param);
    }

    @Override
    public int createTable(String tableName, String... families) {
        return hbaseDao.createTable(tableName, families);
    }

    @Override
    public int dropTable(String tableName) {
        return hbaseDao.dropTable(tableName);
    }

    @Override
    public int dropFamily(String tableName, String family) {
        return hbaseDao.dropFamily(tableName, family);
    }

    @Override
    public int dropNamespace(String... namespaces) {
        return hbaseDao.dropNamespace(namespaces);
    }

}
