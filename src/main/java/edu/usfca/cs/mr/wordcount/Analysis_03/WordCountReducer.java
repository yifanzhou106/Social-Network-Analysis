package edu.usfca.cs.mr.wordcount.Analysis_03;

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
        extends Reducer<Text, ReadableWriter, Text, ReadableWriter> {
    public Map<String,ReadableWriter> map = new HashMap<>();
    private Map<String, Double> wordsTF = new HashMap<>();
    private Map<String, Double> termMap = new HashMap<>();

    @Override
    protected void reduce(
            Text key, Iterable<ReadableWriter> values, Context context)
            throws IOException, InterruptedException {
        ReadableWriter rw = new ReadableWriter();

        ReadableWriter val = values.iterator().next();
            rw = val;
//        map.put(key.toString(), rw);
        context.write(key, rw);

    }

}
