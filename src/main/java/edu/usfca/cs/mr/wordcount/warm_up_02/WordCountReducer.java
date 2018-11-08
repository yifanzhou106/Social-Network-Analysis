package edu.usfca.cs.mr.wordcount.warm_up_02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WordCountReducer
extends Reducer<Text, LongWritable, Text, LongWritable> {
    private long uniqueSubreddit = 0;
    @Override
    protected void reduce(
            Text key, Iterable<LongWritable> values, Context context)
    throws IOException, InterruptedException {
        long count = 0;
        // calculate the total count
        for(LongWritable val : values){
            count += val.get();
        }
        if (count == 1)
            uniqueSubreddit ++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Unique Subreddit is "), new LongWritable(uniqueSubreddit));

    }
}
