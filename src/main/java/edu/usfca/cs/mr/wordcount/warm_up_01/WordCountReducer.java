package edu.usfca.cs.mr.wordcount.warm_up_01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WordCountReducer
extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(
            Text key, Iterable<LongWritable> values, Context context)
    throws IOException, InterruptedException {
        long count = 0;
        // calculate the total count
        for(LongWritable val : values){
            count += val.get();
        }
        context.write(key, new LongWritable(count));
    }

}
