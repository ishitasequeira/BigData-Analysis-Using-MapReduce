package Summarization;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SummarizationReducer extends Reducer<Text, DataWritable,Text, DataWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {
        DataWritable dataWritable = new DataWritable();
        for(DataWritable data : values){
//            System.out.println(stocks.getMax_stock_price_adj_close());

            if(dataWritable.getMax_age()== Integer.MIN_VALUE){
                dataWritable.setMax_age(data.getMax_age());
            }else if(dataWritable.getMax_age()<data.getMax_age()) {
                dataWritable.setMax_age(data.getMax_age());
            }

            if(dataWritable.getMin_age()== Integer.MAX_VALUE){
                dataWritable.setMin_age(data.getMin_age());
            }else if(dataWritable.getMin_age()>data.getMin_age()) {
                dataWritable.setMin_age(data.getMin_age());
            }

            if(dataWritable.getMax_chol()== Integer.MIN_VALUE){
                dataWritable.setMax_chol(data.getMax_chol());
            }else if(dataWritable.getMax_chol()<data.getMax_chol()) {
                dataWritable.setMax_chol(data.getMax_chol());
            }

            if(dataWritable.getMin_chol()== Integer.MAX_VALUE){
                dataWritable.setMin_chol(data.getMin_chol());
            }else if(dataWritable.getMin_chol()>data.getMin_chol()) {
                dataWritable.setMin_chol(data.getMin_chol());
            }

            if(dataWritable.getMax_data_value()== Integer.MIN_VALUE){
                dataWritable.setMax_data_value(data.getMax_chol());
            }else if(dataWritable.getMax_chol()<data.getMax_chol()) {
                dataWritable.setMax_chol(data.getMax_chol());
            }

            if(dataWritable.getMin_data_value()== Integer.MAX_VALUE){
                dataWritable.setMin_data_value(data.getMin_data_value());
            }else if(dataWritable.getMin_data_value()>data.getMin_data_value()) {
                dataWritable.setMin_data_value(data.getMin_data_value());
            }

            if(dataWritable.getMax_thalach()== Integer.MIN_VALUE){
                dataWritable.setMax_thalach(data.getMax_thalach());
            }else if(dataWritable.getMax_thalach()<data.getMax_thalach()) {
                dataWritable.setMax_thalach(data.getMax_thalach());
            }

            if(dataWritable.getMin_thalach()== Integer.MAX_VALUE){
                dataWritable.setMin_thalach(data.getMin_thalach());
            }else if(dataWritable.getMin_thalach()>data.getMin_thalach()) {
                dataWritable.setMin_thalach(data.getMin_thalach());
            }
        }
        context.write(key, dataWritable);
    }
}
