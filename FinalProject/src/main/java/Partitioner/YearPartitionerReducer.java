package Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class YearPartitionerReducer extends Reducer<IntWritable, Text,Text, NullWritable> {


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    for (Text v : values) {
      context.write(v, NullWritable.get());
        }

        }
}
