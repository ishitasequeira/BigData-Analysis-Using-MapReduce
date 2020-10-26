package Summarization;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataWritable implements Writable {

    // define the keys that need to be there
    private float max_data_value;
    private float min_data_value;
    private int min_age;
    private int max_age;
    private int min_chol;
    private int max_chol;
    private int max_thalach;
    private int min_thalach;

    public DataWritable() {
        this.min_data_value = Integer.MAX_VALUE;
        this.max_data_value = Integer.MIN_VALUE;
        this.min_age = Integer.MIN_VALUE;
        this.max_age = Integer.MAX_VALUE;
        this.min_chol = Integer.MAX_VALUE;
        this.max_chol = Integer.MIN_VALUE;
        this.min_thalach = Integer.MAX_VALUE;
        this.max_thalach = Integer.MIN_VALUE;
    }

    public float getMax_data_value() {
        return max_data_value;
    }

    public void setMax_data_value(float max_data_value) {
        this.max_data_value = max_data_value;
    }

    public float getMin_data_value() {
        return min_data_value;
    }

    public void setMin_data_value(float min_data_value) {
        this.min_data_value = min_data_value;
    }

    public int getMin_age() {
        return min_age;
    }

    public void setMin_age(int min_age) {
        this.min_age = min_age;
    }

    public int getMax_age() {
        return max_age;
    }

    public void setMax_age(int max_age) {
        this.max_age = max_age;
    }

    public int getMin_chol() {
        return min_chol;
    }

    public void setMin_chol(int min_chol) {
        this.min_chol = min_chol;
    }

    public int getMax_chol() {
        return max_chol;
    }

    public void setMax_chol(int max_chol) {
        this.max_chol = max_chol;
    }

    public int getMax_thalach() {
        return max_thalach;
    }

    public void setMax_thalach(int max_thalach) {
        this.max_thalach = max_thalach;
    }

    public int getMin_thalach() {
        return min_thalach;
    }

    public void setMin_thalach(int min_thalach) {
        this.min_thalach = min_thalach;
    }

    public void write(DataOutput dataOutput) throws IOException {

        WritableUtils.writeString(dataOutput, String.valueOf(min_data_value));
        WritableUtils.writeString(dataOutput,String.valueOf(max_data_value));
        WritableUtils.writeString(dataOutput,String.valueOf(min_age));
        WritableUtils.writeString(dataOutput,String.valueOf(max_age));
        WritableUtils.writeString(dataOutput, String.valueOf(min_chol));
        WritableUtils.writeString(dataOutput,String.valueOf(max_chol));
        WritableUtils.writeString(dataOutput,String.valueOf(min_thalach));
        WritableUtils.writeString(dataOutput,String.valueOf(max_thalach));
    }

    public void readFields(DataInput dataInput) throws IOException {

        min_data_value = Float.parseFloat(WritableUtils.readString(dataInput));
        max_data_value = Float.parseFloat(WritableUtils.readString(dataInput));
        min_age = Integer.parseInt(WritableUtils.readString(dataInput));
        max_age = Integer.parseInt(WritableUtils.readString(dataInput));
        min_chol = Integer.parseInt(WritableUtils.readString(dataInput));
        max_chol = Integer.parseInt(WritableUtils.readString(dataInput));
        min_thalach = Integer.parseInt(WritableUtils.readString(dataInput));
        max_thalach = Integer.parseInt(WritableUtils.readString(dataInput));
    }

    @Override
    public String toString() {
        return "max_data_value=" + max_data_value +
                ", min_data_value=" + min_data_value +
                ", min_age=" + min_age +
                ", max_age=" + max_age +
                ", min_chol=" + min_chol +
                ", max_chol=" + max_chol +
                ", max_thalach=" + max_thalach +
                ", min_thalach=" + min_thalach ;
    }
}
