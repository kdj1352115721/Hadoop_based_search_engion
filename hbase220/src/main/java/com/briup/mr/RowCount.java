package com.briup.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class RowCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RowCount(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //设置连接参数
        conf.set("hbase.zookeeper.quorum","node1:2181");
        //获取job对象
        Job hbase_job = Job.getInstance(conf, "hbase_job");
        //指定driver类
        hbase_job.setJarByClass(this.getClass());
        //指定mapper
        TableMapReduceUtil.initTableMapperJob(conf.get("inpath"),
           new Scan(),
           RCMapper.class,
           Text.class,
           IntWritable.class,
           hbase_job);
        //指定reducer
        TableMapReduceUtil.initTableReducerJob(conf.get("outpath"),
           RCReducer.class,
           hbase_job);

        hbase_job.waitForCompletion(true);
        return 0;
    }

    //  keyin 行键   valuein result单元格信息
    public static  class  RCMapper extends TableMapper<Text, IntWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            TableSplit tableSplit = (TableSplit)context.getInputSplit();
            byte[] tableName = tableSplit.getTableName();

            context.write(new Text(tableName),new IntWritable(1));
/*            Put put = new Put(key.get());

            Cell[] cells = value.rawCells();
            for (Cell cell : cells) {
                if("info:name".equals(Bytes.toString(cell.getQualifierArray()))){
                    put.add(cell);
                }
            }
            context.write(key,put);*/
            //通过输入分片去拿表名
            //表名  1
        }
    }
    //keyout 不会输出到数据库里，  valueout mutaion(子类 ：put、appande、delete....)
    public static class RCReducer extends TableReducer<Text,IntWritable, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Stream<IntWritable> stream = StreamSupport.stream(values.spliterator(), false);
            long count = stream.count();
            Put put = new Put(key.getBytes());
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("number"),Bytes.toBytes(count+""));
            context.write(NullWritable.get(),put);

        }
    }



}
