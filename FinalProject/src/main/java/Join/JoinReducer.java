package Join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        String result = "";
        int count = 0;
        double avg = 0.0;
        for (Text t : values)
        {
            if(t.toString().contains(".")){
                avg = Double.parseDouble(t.toString());
            } else {
                count = Integer.parseInt(t.toString());
            }
        }
        result = count + "\t" + avg;
        context.write(key, new Text(result));
    }
}
