package edu.usfca.cs.mr.wordcount.Analysis_03;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.security.Key;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WordCountReducer
        extends Reducer<Text, ReadableWriter, Text, DoubleWritable> {
    public Map<String, ReadableWriter> map = new HashMap<>();

    private List<String> allTermMap = new LinkedList<>();
    private List<Double> allTFMap = new LinkedList<>();
//    private List<Map<String, Double>> tfIDFMap = new LinkedList<>();
    private Map<String, Long> wordAppearCountMap = new HashMap<>();
//    private Map<String, Long> termMap;
//    private Map<String, Double> wordsTF;
    private Long commentCount = 0L;

//    private MapWritable map1;
//    private MapWritable map2;

    @Override
    protected void reduce(
            Text key, Iterable<ReadableWriter> values, Context context)
            throws IOException, InterruptedException {
        List<Double> termCountList = new LinkedList<>();

        commentCount++;
        long commentWordsCount = 0;
        ReadableWriter rw = values.iterator().next();
//        map1 = rw.getTermMap();
//        map2 = rw.getTFMap();
        Text[] keylist = (Text[])rw.getKeyList().toArray();
        DoubleWritable[] termlist = (DoubleWritable[])rw.getTermList().toArray();

//        for (MapWritable.Entry entry : map1.entrySet()) {
////            context.write(new Text(entry.getKey() + "_Term"), new DoubleWritable(Double.valueOf(entry.getValue().toString())));
//            commentWordsCount += Double.valueOf(entry.getValue().toString());
//            allTermMap.add(entry.getKey().toString());
//            termCountList.add(Double.valueOf(entry.getValue().toString()));
//        }
        for (Text entry : keylist) {
            allTermMap.add(entry.toString());
        }
        for (DoubleWritable entry : termlist) {
            commentWordsCount += Double.valueOf(entry.toString());
            termCountList.add(Double.valueOf(entry.toString()));
        }

        for (Double count: termCountList){
//            context.write(new Text( "_TF"), new DoubleWritable(count/commentWordsCount));
            allTFMap.add(count/commentWordsCount);
        }

//        for (MapWritable.Entry entry : map2.entrySet()) {
//            context.write(new Text(entry.getKey() + "_TF"), new DoubleWritable(Double.valueOf(entry.getValue().toString())));
//
//            allTFMap.add(Double.valueOf(entry.getValue().toString()));
//        }

        long count;
        for (Text entry : keylist) {
            if (!wordAppearCountMap.containsKey(entry.toString())) {
                wordAppearCountMap.put(entry.toString(), 1L);
            } else {
                count = wordAppearCountMap.get(entry.toString());
                wordAppearCountMap.put(entry.toString(), count + 1L);
            }
        }

//        map.put(key.toString(), rw);
//        context.write(key, rw);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new Text("Total size"), new DoubleWritable(commentCount));
        for (int i = 0; i < allTermMap.size(); i++ ) {
            String key = allTermMap.get(i);
            Double TF = allTFMap.get(i);

            long count = wordAppearCountMap.get(key);
            if (count != 0) {
                double idf = Math.log10(commentCount / count);
                double TF_IDF = TF * idf;
//            context.write(new Text(key + "_TF"), new DoubleWritable(TF));
//            context.write(new Text(key + "_IDF"), new DoubleWritable(idf));

//                wordsIDF.put(termMap.getKey(), idf * termMap.getValue());
                context.write(new Text(key), new DoubleWritable(TF_IDF));
            }
//            tfIDFMap.add(wordsIDF);
        }

    }
}
