package search_engine.mapreduce.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Pagerank extends Configured implements Tool {
    //定义初始权重值
    private static double weights = 10;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Pagerank(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        //设置连接对象
        conf.set("hbase.zookeeper.quorum","172.16.0.4:2181,172.16.0.5:2181,172.16.0.6:2181,172.16.0.7:2181");
        //设置job
        Job job = Job.getInstance(conf, "PageRank_1");
        //为job指定driver类
        job.setJarByClass(this.getClass());
        //指定mapper
        TableMapReduceUtil.initTableMapperJob(conf.get("inpath"),
           new Scan(),
           PageMapper.class,
           Text.class,
           Text.class,
           job
        );
        TableMapReduceUtil.initTableReducerJob(conf.get("outpath"),
           PageReducer.class,
           job
           );
        job.waitForCompletion(true);
        return 0;
    }

    public static class PageMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // ol 中含有内容和个数
            byte[] ol_value = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("ol"));
            // il 中只含有个数
            byte[] il_value = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("il"));
            // url
            byte[] url_value = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("url"));

            String url = url_value.toString().trim();

            String[] splits = ol_value.toString().split(",");
            StringBuffer url_weights_buffer = new StringBuffer();
            //对 出链接 操作
            if(splits.length ==2){
                String s = splits[0].trim();
                url_weights_buffer.append(url).append(","+String.valueOf(weights));
            }
            //  key: url,权重   value: ol
            context.write(new Text(url_weights_buffer.toString()),new Text(splits[0]+","));
        }
    }

//    Rowkey    		Url：rank， rul：rank，url：rank

    public static class PageReducer extends TableReducer<Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Put put = null;
            double rank =0;
            double d = 0.85;
            // value中的多个ol 此时看做是il来操作，
            String[] valuesplits = values.toString().split(",");
            for (String valuesplit : valuesplits) {
                //单算一个页面的权重值
                Text value = values.iterator().next();
                //拆分拿到url和当前页面权重值
                String[] keysplits = key.toString().split(",");
                //拿到当前内个进页面得到的权重值
                double cWeights = Double.parseDouble(keysplits[1])/valuesplits.length;
                rank = (1-d)+d*cWeights;
                put = new Put(Bytes.toBytes(valuesplit));
            }
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("rank"),Bytes.toBytes(rank));
            context.write(NullWritable.get(),put);
        }
    }
}
