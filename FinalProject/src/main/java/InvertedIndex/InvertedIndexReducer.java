package InvertedIndex;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class InvertedIndexReducer extends Reducer<Text, Text,Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> result = new HashSet();
        for(Text is :values){
            result.add(is.toString());
        }

        context.write(key,new Text(result.toString()));
    }
}
