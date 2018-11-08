package edu.usfca.cs.mr.wordcount.Analysis_01_b;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ScreamerWriter implements Writable{
    private LongWritable totalCount;
    private LongWritable screamerCount;
    private Text subreddit;

    public ScreamerWriter() {
        this.totalCount = new LongWritable(0);
        this.screamerCount = new LongWritable(0);
    }

    public ScreamerWriter(String subreddit) {
        this();
        this.subreddit = new Text(subreddit);
    }


    public LongWritable getTotal() {
        return totalCount;
    }


    public LongWritable getScreamerCount() {
        return screamerCount;
    }


    public void setTotal(LongWritable totalCount) {
        this.totalCount = totalCount;
    }

    public void setScreamerCount(LongWritable screamerCount) {
        this.screamerCount = screamerCount;
    }

    public void increment(ScreamerWriter sw) {
        incrementTotal(sw.totalCount.get());
        incrementScreamerCount(sw.screamerCount.get());
    }

    private void incrementTotal(long count) {
        this.totalCount.set(this.totalCount.get() + count);
    }

    public void incrementScreamerCount(long count) {
        this.screamerCount.set(this.screamerCount.get() + count);
    }


    public void incrementTotal() {
        this.incrementTotal(1);
    }

    public void incrementScreamerCount() {
        this.incrementScreamerCount(1);
    }

    public void readFields(DataInput in) throws IOException {
        this.totalCount.readFields(in);
        this.screamerCount.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        this.totalCount.write(out);
        this.screamerCount.write(out);
    }

    public boolean hasCount() {
        return this.totalCount.get() > 0
                || this.screamerCount.get() > 0;
    }

    public double calPercentage(){
        double total = this.totalCount.get();
        if (total==0)
            total=1;
        double scream = this.screamerCount.get();
        return scream/total *100;

    }

    @Override
    public String toString() {
        return  "Total token count:" +totalCount.toString()
                + "\t" + "CAP token count:"+screamerCount.toString()
                + "\t" + "CAP / Total = " + this.calPercentage() + "%";
    }

}
