package search_engine.mapreduce.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.NavigableMap;

public class DataClean extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new DataClean(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //设置连接参数
        conf.set("hbase.zookeeper.quorum","172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        //获取job对象
        Job hbase_job = Job.getInstance(conf, "clean_data");
        //指定driver类
        hbase_job.setJarByClass(this.getClass());
        //指定mapper
        TableMapReduceUtil.initTableMapperJob(conf.get("inpath"),
           new Scan(),
           DataMapper.class,
           Text.class,
           IntWritable.class,
           hbase_job);
        //指定reducer
        TableMapReduceUtil.initTableReducerJob(conf.get("outpath"),
           DataReducer.class,
           hbase_job);

        hbase_job.waitForCompletion(true);
        return 0;
    }

    public static  class  DataMapper extends TableMapper<ImmutableBytesWritable, MapWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Cell cell = value.getColumnLatestCell(Bytes.toBytes("f"), Bytes.toBytes("st"));
// 1.从抓取过的页面中（f:st=2）   选择 列族：列
//2.提取有效字段（url,title,il , ol）  字段
// il 入链接的内容
// ol 出链接的对象，个数
            byte[] valueArray = cell.getValueArray();
            int valueOffset = cell.getValueOffset();
            int valueLength = cell.getValueLength();
            //如果st为2，则说明网页是成功趴取的
            if(Bytes.toInt(valueArray)==2){
                //用于写出的容器
                MapWritable mapWritable = new MapWritable();
                //用于拼接title和url中的标题的容器
                StringBuffer buffer = new StringBuffer();
                //获取url 和网页内容
                Cell cell_bas = value.getColumnLatestCell(Bytes.toBytes("f"), Bytes.toBytes("bas"));
                Cell cell_text = value.getColumnLatestCell(Bytes.toBytes("p"), Bytes.toBytes("c"));
                //获取title
                Cell cell_t = value.getColumnLatestCell(Bytes.toBytes("p"), Bytes.toBytes("t"));
                //拼接成rowkey，需要获得text的a标签中的内容
                buffer.append(cell_text.getValueArray()).append(","+cell_t.getValueArray());

                //===============================以上是凑成的rowkey========================================
                //获取il个数
                NavigableMap<byte[], byte[]> il_family_map = value.getFamilyMap(Bytes.toBytes("il"));
                int il_size = il_family_map.size();
                //                //获取ol的内容和个数
                StringBuffer buffer_ol = new StringBuffer();
                NavigableMap<byte[], byte[]> ol_family_map = value.getFamilyMap(Bytes.toBytes("ol"));
                int ol_size = ol_family_map.size();
                ol_family_map.forEach((t1,t2)-> buffer_ol.append(t2));
                // rowkey : text+title
                mapWritable.put(new BytesWritable("0".getBytes()),new BytesWritable(Bytes.toBytes(buffer.toString())));
                //添加url
                mapWritable.put(new BytesWritable("1".getBytes()),new BytesWritable(cell_bas.getValueArray()));
                //添加il
                mapWritable.put(new BytesWritable("2".getBytes()),new BytesWritable(Bytes.toBytes(il_size)));
                //添加ol
                mapWritable.put(new BytesWritable("3".getBytes()),new BytesWritable(Bytes.toBytes(buffer_ol.toString()+","+String.valueOf(ol_size))));
                context.write(key,mapWritable);
            }

        }
    }
    private static Put put;
    public static  class DataReducer extends TableReducer<ImmutableBytesWritable, MapWritable,NullWritable>{
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            while(values.iterator().hasNext()) {
                MapWritable mapWritable = values.iterator().next();

                //拿到buffer_title 做为rowkey
                put = new Put(Bytes.toBytes(mapWritable.get("0").toString()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("url"), Bytes.toBytes(mapWritable.get("1").toString()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("il"), Bytes.toBytes(mapWritable.get("2").toString()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ol"), Bytes.toBytes(mapWritable.get("3").toString()));
            }
            context.write(NullWritable.get(),put);

        }
    }

}

