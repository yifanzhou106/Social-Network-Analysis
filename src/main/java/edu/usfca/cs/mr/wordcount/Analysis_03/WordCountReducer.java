package edu.usfca.cs.mr.wordcount.Analysis_03;

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
        extends Reducer<Text, ReadableWriter, Text, DoubleWritable> {
    public Map<String, ReadableWriter> map = new HashMap<>();

    private List<Map<String, Long>> allTermMap = new ArrayList<>();
    private List<Map<String, Double>> allTFMap = new ArrayList<>();
    private List<Map<String, Double>> tfIDFMap = new ArrayList<>();
    private Map<String, Long> wordAppearCountMap = new HashMap<>();

    @Override
    protected void reduce(
            Text key, Iterable<ReadableWriter> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Long> termMap = new HashMap<>();
        Map<String, Double> wordsTF = new HashMap<>();

        ReadableWriter rw = values.iterator().next();
        MapWritable map1 = rw.getTermMap();
        MapWritable map2 = rw.getTFMap();

        for (MapWritable.Entry entry : map1.entrySet()) {
            termMap.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
        }
        for (MapWritable.Entry entry : map2.entrySet()) {
            wordsTF.put(entry.getKey().toString(), Double.valueOf(entry.getValue().toString()));
        }

        for (Map.Entry<String,Long> entry: termMap.entrySet()){
            if (!wordAppearCountMap.containsKey(entry.getKey())){
                wordAppearCountMap.put(entry.getKey(),1L);
            }else{
                long count = wordAppearCountMap.get(entry.getKey());
                wordAppearCountMap.put(entry.getKey(), count + 1L);
            }
        }
        allTermMap.add(termMap);
        allTFMap.add(wordsTF);


//        map.put(key.toString(), rw);
//        context.write(key, rw);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        long totalComments = allTermMap.size();
        context.write(new Text("Total size"), new DoubleWritable(totalComments));
        Map<String, Double> wordsIDF;
//        for (Map<String, Long> map : allTermMap) {
          Map<String, Long> map = allTermMap.get(0);
            wordsIDF = new HashMap<>();
            for (Map.Entry<String, Long> termMap : map.entrySet()) {
              long count = wordAppearCountMap.get(termMap.getKey());
                double idf = Math.log(totalComments/count);
//                context.write(new Text(termMap.getKey() + "_count"), new DoubleWritable(count));
//                context.write(new Text(termMap.getKey() + "_log(totalComments/count)"), new DoubleWritable(Math.log(totalComments/count)));
                wordsIDF.put(termMap.getKey(), idf * termMap.getValue());
            }
            tfIDFMap.add(wordsIDF);
//        }
        /**
         * Print all
         */

        for (Map<String, Double> tfIDFmap :tfIDFMap){
            for (Map.Entry<String, Double> result: tfIDFmap.entrySet())
            {
                context.write(new Text(result.getKey()), new DoubleWritable(result.getValue()));
            }
            context.write(new Text("\n*************Next Comment:"), new DoubleWritable(0));

        }
    }
}
