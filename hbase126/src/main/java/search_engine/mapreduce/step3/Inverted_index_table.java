package search_engine.mapreduce.step3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Inverted_index_table extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Inverted_index_table(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        return 0;
    }

    public static class IndexMapper extends TableMapper<Text,Text>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        }
    }
    public static class IndexReducer extends TableReducer<Text,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }
}
