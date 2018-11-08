package edu.usfca.cs.mr.wordcount.Analysis_04;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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
        extends Reducer<Text, SentimentAnalysisWritable, Text, LongWritable> {
    private Map<String, Long> positiveMap = new HashMap<>();
    private Map<String, Long> negativeMap = new HashMap<>();


    @Override
    protected void reduce(
            Text key, Iterable<SentimentAnalysisWritable> values, Context context)
            throws IOException, InterruptedException {

        SentimentAnalysisWritable rw = new SentimentAnalysisWritable();
        for (SentimentAnalysisWritable val : values) {
            rw.increment(val);
        }

        positiveMap.put(key.toString(), rw.getPositiveCount().get());
        negativeMap.put(key.toString(), rw.getNegativeCount().get());
//        context.write(key, rw);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        Set<Map.Entry<String, Long>> set = positiveMap.entrySet();
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1,
                               Map.Entry<String, Long> o2) {
                if ((o2.getValue() < o1.getValue()))
                    return -1;
                else if ((o2.getValue() > o1.getValue()))
                    return 1;
                else return 0;
            }
        });
        context.write(new Text("***************Positive Top "), new LongWritable(5));
        for ( int i = 0; i<5; i++) {
            Map.Entry<String,Long> entry  = list.get(i);
            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
        }
//        for (Map.Entry<String, Long> entry : list) {
//            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
//        }

        set = negativeMap.entrySet();
        list = new ArrayList<Map.Entry<String, Long>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1,
                               Map.Entry<String, Long> o2) {
                if ((o2.getValue() < o1.getValue()))
                    return -1;
                else if ((o2.getValue() > o1.getValue()))
                    return 1;
                else return 0;
            }
        });
        context.write(new Text("\n\n***************Negative Top "), new LongWritable(5));
        for ( int i = 0; i<5; i++) {
            Map.Entry<String,Long> entry  = list.get(i);
            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
        }

//        for (Map.Entry<String, Long> entry : list) {
//            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
//        }

    }
}
