package Partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class YearPartitionerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    public IntWritable year = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        String strDate = tokens[1];
        if (!value.toString().startsWith("I")) {
            year.set(Integer.parseInt(strDate));
            context.write(year, value);
        }
    }
}
