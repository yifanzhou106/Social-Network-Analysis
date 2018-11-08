package edu.usfca.cs.mr.wordcount.warm_up_03_b;

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
        extends Reducer<Text, LongWritable, Text, LongWritable> {
    private Map<String,Integer> map = new HashMap<String, Integer>();

    @Override
    protected void reduce(
            Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(LongWritable val : values){
            count += val.get();
        }
        map.put(key.toString(),count);
//        context.write(key, new IntWritable(count));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Set<Map.Entry<String, Integer>> set = map.entrySet();
        List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return -o2.getValue() +o1.getValue();
            }
        });

        for ( int i = 0; i<3; i++) {
            Map.Entry<String,Integer> entry  = list.get(i);
            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
        }

    }
}
