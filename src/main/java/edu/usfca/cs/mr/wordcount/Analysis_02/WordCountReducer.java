package edu.usfca.cs.mr.wordcount.Analysis_02;

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

    @Override
    protected void reduce(
            Text key, Iterable<ReadableWriter> values, Context context)
            throws IOException, InterruptedException {
        ReadableWriter rw = new ReadableWriter();
        for(ReadableWriter val : values){
            rw.increment(val);
        }
        map.put(key.toString(), rw);
//        context.write(key, rw);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<String,ReadableWriter>> set = map.entrySet();
        List<Map.Entry<String,ReadableWriter>> list = new ArrayList<Map.Entry<String, ReadableWriter>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String,ReadableWriter>>() {
            public int compare(Map.Entry<String,ReadableWriter> o1,
                               Map.Entry<String,ReadableWriter> o2) {
                if ((o2.getValue().getFleschReadingEase() < o1.getValue().getFleschReadingEase()) )
                    return 1;
                else if((o2.getValue().getFleschReadingEase() > o1.getValue().getFleschReadingEase()))
                    return -1;
                else return 0;
            }
        });
        for ( int i=0;i<3;i++) {
            Map.Entry<String,ReadableWriter> entry  = list.get(i);
            context.write(new Text(entry.getKey()),entry.getValue());
        }
//        for(Map.Entry<String,ScreamerWriter> entry :list)
//        {
//            context.write(new Text(entry.getKey()),entry.getValue());
//        }
    }
}
