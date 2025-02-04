package Join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.HashMap;

public class JoinMapper1 extends Mapper<LongWritable,Text,Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
          String record = value.toString();
          String[] parts = record.split("\t");
          context.write(new Text(parts[0]), new Text(parts[1]));
    }
}
