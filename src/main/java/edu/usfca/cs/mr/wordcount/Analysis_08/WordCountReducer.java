package edu.usfca.cs.mr.wordcount.Analysis_08;

import org.apache.hadoop.io.DoubleWritable;
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
        extends Reducer<Text, MusicWritable, Text, DoubleWritable> {
    public Map<String, edu.usfca.cs.mr.wordcount.Analysis_03.ReadableWriter> map = new HashMap<>();

    private List<Map<String, Long>> allTermMap = new ArrayList<>();
    private List<Map<String, Double>> allTFMap = new ArrayList<>();
    private List<Map<String, Double>> tfIDFMap = new ArrayList<>();

    @Override
    protected void reduce(
            Text key, Iterable<MusicWritable> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Long> termMap = new HashMap<>();
        Map<String, Double> wordsTF = new HashMap<>();

        MusicWritable rw = values.iterator().next();



//        map.put(key.toString(), rw);
//        context.write(key, rw);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long totalComments = allTermMap.size();
        context.write(new Text("Total size"), new DoubleWritable(totalComments));

        for (Map<String, Long> map : allTermMap) {
            Map<String, Double> wordsIDF = new HashMap<>();

            for (Map.Entry<String, Long> termMap : map.entrySet()) {
                long count = 0;
                for (Map<String, Long> checkmap : allTermMap) {
                    for (Map.Entry<String, Long> entry : checkmap.entrySet()) {
                        if (entry.getKey().equals(termMap.getKey()))
                            count++;
                    }
                }
                double idf = Math.log(totalComments/count);
//                context.write(new Text(termMap.getKey() + "_count"), new DoubleWritable(count));
//                context.write(new Text(termMap.getKey() + "_log(totalComments/count)"), new DoubleWritable(Math.log(totalComments/count)));

                wordsIDF.put(termMap.getKey(), idf * termMap.getValue());
            }
            tfIDFMap.add(wordsIDF);
        }
        /**
         * Print all
         */

        for (Map<String, Double> map :tfIDFMap){
            for (Map.Entry<String, Double> result: map.entrySet())
            {
                context.write(new Text(result.getKey()), new DoubleWritable(result.getValue()));
            }

        }
    }
}
