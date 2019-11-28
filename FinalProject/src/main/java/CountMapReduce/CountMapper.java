package CountMapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       String str  = String.valueOf(value);
       String [] tokens = str.split(",");
       //count according to topic
        context.write(new Text(tokens[5]),new IntWritable(1));
    }
}
