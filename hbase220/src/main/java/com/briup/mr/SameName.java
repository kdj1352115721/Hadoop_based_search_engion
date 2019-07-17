package com.briup.mr;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class SameName extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SameName(),args);
        //args是一个数组,为了能够解析后续中的 -D inpath -lib xxxxx 之类的附加值
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //设置连接的hbase ，hbase的信息保存在zookeeper中
        conf.set("hbase.zookeeper.quorum","node1:2181,node2:2181,node3:2181");

        //设置job任务
        Job job = Job.getInstance(conf, "SameName");

        //为job指定driver类
        job.setJarByClass(this.getClass());
        //装配job 的mapper ,reducer
        TableMapReduceUtil.initTableMapperJob(conf.get("inpath"),
           new Scan(),
           SMapper.class,
           ImmutableBytesWritable.class,
           Put.class,
           job
           );
        TableMapReduceUtil.initTableReducerJob(conf.get("outpath"),
           SReducer.class,
           job
           );
        job.waitForCompletion(true);
        return 0;
    }
    public static class SMapper extends TableMapper<ImmutableBytesWritable, Put>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Put put = new Put(key.get());
            Cell[] cells = value.rawCells();
            for (Cell cell : cells) {
                //列名为wei的留下，只比对了列名  ，但是列名是和列族并存的
                if("wei".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    put.add(cell);
                }
            }
            context.write(key,put);
        }
    }
    public static class SReducer extends TableReducer<ImmutableBytesWritable,Put, NullWritable>{
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
/*            values.forEach(value-> {
                try {
                    context.write(NullWritable.get(),value);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });*/
            for(Put put :values){
                context.write(NullWritable.get(),put);
            }

        }
    }


}
