package personal.liyitong.hadoop.hbase.rowkey;

public class TimestampGenerator implements RowKeyGenerator {

    private String prefix;

    public TimestampGenerator(String prefix) {
        this.prefix = prefix;
    }

    public TimestampGenerator() {
    }

    @Override
    public String generateRowKey() {
        if (prefix != null) {
            return prefix + System.currentTimeMillis();
        } else {
            return String.valueOf(System.currentTimeMillis());
        }
    }
}
