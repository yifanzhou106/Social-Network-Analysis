package edu.usfca.cs.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WordCountReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Text,Integer> map = new TreeMap();

    public static <K, V extends Comparable<V>> Map<K, V>
    sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator =
                new Comparator<K>() {
                    public int compare(K k1, K k2) {
                        int compare =
                                map.get(k1).compareTo(map.get(k2));
                        if (compare == 0)
                            return 1;
                        else
                            return compare;
                    }
                };

        Map<K, V> sortedByValues =
                new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }

    @Override
    protected void reduce(
            Text key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
        }
        map.put(key,count);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text,Integer> sortedMap = sortByValues(map);
        Map.Entry<Text,Integer> entry = sortedMap.entrySet().iterator().next();
        context.write(entry.getKey(), new IntWritable(entry.getValue()));
        super.cleanup(context);
    }
}
