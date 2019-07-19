package com.briup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Step1_count extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Step1_count(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        //利用configured的静态getconf（）方法，得到Configuration
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "step1_count");
        //设定driver类,也就是主类
        job.setJarByClass(this.getClass());
        //装配jobd的 mapper reducer
        job.setMapperClass(CountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //装配job 的输入分割格式 ， 输出分割格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //从命令行参数获取输入输出路径
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }
    public static class CountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            context.write(new Text(splits[0]),new IntWritable(1));

        }
    }
    // <1,1>
    // <2,1>
    //<1.1> .......
    // map 之后 ，经过suffle，相同key只取一个， 形成<key,value-list> 形式的键值对，如 <1,1 1 > <2,1> ，进入ruduce
    public static class CountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int countsum = 0;
            for (IntWritable value : values) {
                countsum+=value.get();
            }
            context.write(key,new IntWritable(countsum));
        }
    }

}
