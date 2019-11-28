package Top10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top10Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private final IntWritable ONE = new IntWritable(1);



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String ipAddress = value.toString().split(" ")[0];
        context.write(new Text(ipAddress),new Text(String.valueOf(ONE)));
    }
}
