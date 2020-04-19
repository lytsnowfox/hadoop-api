package personal.liyitong.hadoop.hbase.entity;

import com.alibaba.fastjson.JSONArray;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;

import java.io.IOException;
import java.util.List;

public class HBaseParam {

    private String table;
    private String family;
    private String rowKey;
    // 单个数据key: 列名称，value: 对应值
    // 对应的数据列表，对应多个元素，用于写入
    private JSONArray array;

    private Integer page;
    private Integer pageSize;
    private Scan scan;
    private FilterList filterList;
    private List<String> qualifiers;

    public HBaseParam() {
        this.scan = new Scan();
        this.filterList = new FilterList();
        this.scan.setFilter(this.filterList);
        this.array = new JSONArray();
    }

    public HBaseParam(String table, String rowKey, String family) {
        this();
        this.table = table;
        this.family = family;
        this.rowKey = rowKey;
    }

    public HBaseParam(String table) {
        this(table, null, null);
    }

    public HBaseParam(String table, String family) {
        this(table, null, family);
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public JSONArray getArray() {
        return array;
    }

    public void setArray(JSONArray array) {
        this.array = array;
    }

    public Integer getPage() {
        return page;
    }

    public void setPage(Integer page) {
        this.page = page;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public Scan getScan() {
        return scan;
    }

    public void setScan(Scan scan) {
        this.scan = scan;
    }

    public FilterList getFilterList() {
        return filterList;
    }

    public void setFilterList(FilterList filterList) {
        this.filterList = filterList;
    }

    public List<String> getQualifiers() {
        return qualifiers;
    }

    public void setQualifiers(List<String> qualifiers) {
        this.qualifiers = qualifiers;
    }

    //==============工具方法===============
    // 新增带插入的数据
    public HBaseParam addData(Object t) {
        this.array.add(t);
        return this;
    }

    // 设置分页
    public HBaseParam usePager(String startRow, Integer pageSize) {
        this.getScan().setStartRow(startRow.getBytes());
        Filter pageFilter = new PageFilter(pageSize);
        this.filterList.addFilter(pageFilter);
        return this;
    }

    public HBaseParam addFilter(Filter filter) {
        this.filterList.addFilter(filter);
        return this;
    }

    public boolean resetFilters() {
        try {
            this.filterList.reset();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
