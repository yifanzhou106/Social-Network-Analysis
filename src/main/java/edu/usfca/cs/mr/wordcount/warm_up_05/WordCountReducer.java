package edu.usfca.cs.mr.wordcount.warm_up_05;

import org.apache.hadoop.io.DoubleWritable;
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
        extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private Map<String, Map<String, Integer>> yearMap = new HashMap<>();

    @Override
    protected void reduce(
            Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for (IntWritable val : values) {
            count += val.get();
        }
        String year = key.toString().split("_")[0];
        String month = key.toString().split("_")[1];
        if (!yearMap.containsKey(year)) {
            Map<String, Integer> monthCountMap = new HashMap<>();
            monthCountMap.put(month, count);
            yearMap.put(year, monthCountMap);
        } else {
            Map<String, Integer> monthCountMap = yearMap.get(year);
            monthCountMap.put(month, count);
            yearMap.put(year, monthCountMap);
        }
//        double doublecount = count;
//        context.write(key, new DoubleWritable(doublecount));


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, Double> resultMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> yearEntry : yearMap.entrySet()) {

            Map<String, Integer> monthCountMap = yearEntry.getValue();
            double max = -1;
            double min = -1;
            String year = yearEntry.getKey();
            for (Map.Entry<String, Integer> monthEntry : monthCountMap.entrySet()) {
                if (max == -1 && min == -1) {
                    max = monthEntry.getValue();
                    min = monthEntry.getValue();
                }
                if (monthEntry.getValue() > max)
                    max = monthEntry.getValue();
                if (monthEntry.getValue() < min)
                    min = monthEntry.getValue();
            }
            context.write(new Text("Max: "), new DoubleWritable(max));
            context.write(new Text("Min: "), new DoubleWritable(min));

            double denominator = max - min;
            if (denominator == 0)
                denominator = 1;

            for (Map.Entry<String, Integer> monthEntry : monthCountMap.entrySet()) {
                String month = monthEntry.getKey();
                double numerator = monthEntry.getValue() - min;
                double result = numerator / denominator;
                resultMap.put(year + "_" + month, result);
            }
        }
        for (Map.Entry<String, Double> entry : resultMap.entrySet()) {
            context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
        }

    }
}
