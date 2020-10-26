package Top10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Top10Reducer  extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for(Text i : values) {
            String [] tokens = i.toString().split(",");
            sum += Integer.parseInt(tokens[0]);
            count+= Integer.parseInt(tokens[1]);
        }
//        System.out.println(new String(String.valueOf(key)));
        float avg = (sum*1.0f)/count;
        System.out.println(key + " --> " + avg);
        context.write(key, new Text(String.valueOf(avg)));
    }
}