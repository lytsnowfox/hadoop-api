package personal.liyitong.hadoop.hbase.rowkey;

import java.util.UUID;

public class UUIDGenerator implements RowKeyGenerator {

    @Override
    public String generateRowKey() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}
