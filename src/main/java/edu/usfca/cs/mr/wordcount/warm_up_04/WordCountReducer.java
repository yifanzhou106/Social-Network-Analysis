package edu.usfca.cs.mr.wordcount.warm_up_04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WordCountReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }

}
