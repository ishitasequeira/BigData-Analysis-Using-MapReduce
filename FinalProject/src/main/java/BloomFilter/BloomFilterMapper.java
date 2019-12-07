package BloomFilter;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
    Funnel<StateTopic> speakerFunnel =
            new Funnel<StateTopic>() {
                public void funnel(StateTopic person, Sink into) {
                    into.putString(person.sex, Charsets.UTF_8)
                            .putString(person.naturalLanguage, Charsets.UTF_8);
                }
            };
    private BloomFilter<StateTopic> stateTopicBloomFilter =
            BloomFilter.create(speakerFunnel, 500, 0.1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        StateTopic sp1 = new StateTopic("MA", "Hypertension");
        StateTopic sp2 = new StateTopic("MA", "Smoking");
        StateTopic sp3 = new StateTopic("NY", "Diabetes");
        StateTopic sp4 = new StateTopic("NY", "Stroke");
        ArrayList<StateTopic> stateTopicList = new ArrayList<StateTopic>();
        stateTopicList.add(sp1);
        stateTopicList.add(sp2);
        stateTopicList.add(sp3);
        stateTopicList.add(sp4);
        for (StateTopic spr : stateTopicList) {
            stateTopicBloomFilter.put(spr);
        }
    }

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (!value.toString().startsWith("Y")) {
            String[] values = value.toString().split(",");
            StateTopic sp = new StateTopic(values[1], values[5]);
            if (stateTopicBloomFilter.mightContain(sp)) {
                context.write(value, NullWritable.get());
            }
        }
    }
}