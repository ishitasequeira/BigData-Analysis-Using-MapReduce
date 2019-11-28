package Partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<IntWritable,Text> implements Configurable {

    private static final  String MIN_LAST_ACCESS_DATE_YEAR = "min.last.access.date.year";
    private Configuration configuration = null;
    private int minLastAccessDateYear = 0;


    public int getPartition(IntWritable key, Text value, int numPartitions) {
        return key.get()-minLastAccessDateYear;
    }

    public void setConf(Configuration configuration) {
            this.configuration = configuration;
            minLastAccessDateYear = this. configuration.getInt(MIN_LAST_ACCESS_DATE_YEAR,0);

    }

    public Configuration getConf() {
        return null;
    }



    public static void setMinLastAccessDateYear(Job job, int minLastAccessDateYear) {
        job.getConfiguration().setInt(MIN_LAST_ACCESS_DATE_YEAR,minLastAccessDateYear);
    }
}
