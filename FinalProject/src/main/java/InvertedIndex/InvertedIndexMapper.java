package InvertedIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String str  = String.valueOf(value);
        String [] tokens = str.split(",");
        //count according to indicator
        if(!tokens[0].startsWith("I"))
        context.write(new Text(tokens[7]),new Text(tokens[1]));
    }
}
