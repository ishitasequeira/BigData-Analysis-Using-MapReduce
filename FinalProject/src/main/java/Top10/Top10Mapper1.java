package Top10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top10Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        if (!value.toString().startsWith("Y")) {
            String location = value.toString().split(",")[2];
            String chol = value.toString().split(",")[14];
            System.out.println(chol+",1");
            context.write(new Text(location), new Text(chol + ",1"));
        }
    }
}
