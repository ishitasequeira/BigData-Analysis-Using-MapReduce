package SecondarySorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    if (!value.toString().startsWith("Y")) {
      String[] values = value.toString().split(",");
      String location = null;
      String topic = null;

      try {
        location = values[2];
        topic = values[5];
      } catch (Exception e) { }

      if (null != location || null != topic) {
        try {
          CompositeKeyWritable cw = new CompositeKeyWritable(location, topic);
          context.write(cw, NullWritable.get());
        } catch (Exception e) { }
      }
    }
  }
}
