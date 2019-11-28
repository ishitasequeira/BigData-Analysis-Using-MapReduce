package Top10;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

public class TopMapper2 extends Mapper<Text, Text, NullWritable, Text> {
    TreeMap<Float, Text> map;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new TreeMap<Float, Text>();
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String ip =key.toString();
//        System.out.println("key"+key.toString());
//        System.out.println("value:"+value.toString());
        float avg = Float.parseFloat(value.toString());
//        System.out.println(new Text(ip+","+avg));
        map.put(avg, new Text(ip+","+avg));
        if(map.size()>10) {
            map.remove(map.firstKey());
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Text t : map.descendingMap().values()) {
//           System.out.println("MAP "+t);
            context.write(NullWritable.get(),t);
        }
    }
}