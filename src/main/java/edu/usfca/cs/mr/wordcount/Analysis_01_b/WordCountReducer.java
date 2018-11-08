package edu.usfca.cs.mr.wordcount.Analysis_01_b;

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
        extends Reducer<Text, ScreamerWriter, Text, ScreamerWriter> {
    public Map<String,ScreamerWriter> map = new HashMap<>();

    @Override
    protected void reduce(
            Text key, Iterable<ScreamerWriter> values, Context context)
            throws IOException, InterruptedException {
        ScreamerWriter sw = new ScreamerWriter();
        for(ScreamerWriter val : values){
            sw.increment(val);
        }
        map.put(key.toString(), sw);
//        context.write(key, sw);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<String,ScreamerWriter>> set = map.entrySet();
        List<Map.Entry<String,ScreamerWriter>> list = new ArrayList<Map.Entry<String, ScreamerWriter>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String,ScreamerWriter>>() {
            public int compare(Map.Entry<String,ScreamerWriter> o1,
                               Map.Entry<String,ScreamerWriter> o2) {
                if ((o2.getValue().getScreamerCount().get() - o1.getValue().getScreamerCount().get()) > 0L)
                    return 1;
                else if((o2.getValue().getScreamerCount().get() - o1.getValue().getScreamerCount().get()) < 0L)
                    return -1;
                else return 0;
            }
        });
        for ( int i=0;i<5;i++) {
            Map.Entry<String,ScreamerWriter> entry  = list.get(i);
            context.write(new Text(entry.getKey()),entry.getValue());
        }
//        for(Map.Entry<String,ScreamerWriter> entry :list)
//        {
//            context.write(new Text(entry.getKey()),entry.getValue());
//        }
    }
}