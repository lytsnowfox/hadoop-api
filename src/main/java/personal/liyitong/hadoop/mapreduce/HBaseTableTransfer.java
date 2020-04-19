package personal.liyitong.hadoop.mapreduce;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import personal.liyitong.hadoop.config.HBaseConfig;

import java.io.IOException;

public class HBaseTableTransfer extends Configured implements Tool {

    private String source;

    private String destination;

    public HBaseTableTransfer(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

    /**
     * map实现输入输出均为<K, V>形式，K,V均实现Writable接口
     * 内部类必须为static，否则会有NoSuchMethodException
     */
    public static class Mapper extends TableMapper<Text, Put> {

        private JSONObject transferMap = null;

        /**
         * 正整个表格的所有数据都会经由map处理
         * @param key       行键
         * @param value     值，hbase中一行内容
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            JSONObject tableMap = getTransferMap(context);
            String desFamily = null;
            // 只写入存在于映射关系中的列族
            for (String sourceFamily : tableMap.keySet()) {
                if (!value.getFamilyMap(Bytes.toBytes(sourceFamily)).isEmpty()) {
                    desFamily = tableMap.getString(sourceFamily);
                    break;
                }
            }
            if (desFamily == null) {
                return;
            }
            String rowKey = Bytes.toString(key.get());
            Text mapOutputKey = new Text();
            mapOutputKey.set(rowKey);
            Put put = new Put(key.get());
            for (Cell cell : value.listCells()) {
                put.addColumn(Bytes.toBytes(desFamily), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
            }
            // 向<K2, V2>写入数据
            context.write(mapOutputKey, put);
        }

        public synchronized JSONObject getTransferMap(Context context) {
            if (transferMap == null) {
                String mapStr = context.getConfiguration().get(HBaseConfig.TABLEMAP);
                transferMap = JSONObject.parseObject(mapStr);
            }
            return transferMap;
        }
    }

    /**
     * reduce实现
     */
    public static class Reducer extends TableReducer<Text, Put, NullWritable> {
        /**
         * map输出的<K2, V2>中，相同的K2会被合并，此处的输入是<K2, List V2>
         * @param key       K2就是对应map函数中写入的key
         * @param values    根据K2合并后的序列List V2
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put put : values) {
                context.write(NullWritable.get(), put);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        Scan scan = new Scan();
        scan.setCaching(1000);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(
                source,      // input table
                scan,
                Mapper.class,
                Text.class,         // mapper output key
                Put.class,          // mapper output value
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                destination,        // output table
                Reducer.class,
                job
        );

        job.setNumReduceTasks(10);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : -1;
    }


}
