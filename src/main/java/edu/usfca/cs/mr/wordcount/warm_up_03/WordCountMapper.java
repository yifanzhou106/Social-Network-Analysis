package edu.usfca.cs.mr.wordcount.warm_up_03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
static Map<String,String> commitsMap = new HashMap<>();
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());

            // tokenize into words.
//            StringTokenizer itr = new StringTokenizer(value.toString());
            // emit word, count pairs.
//            while (itr.hasMoreTokens()) {
//                context.write(new Text(itr.nextToken()), new IntWritable(1));
//            }
            context.write(new Text(json.get("author").toString()), new IntWritable(1));
        } catch (ParseException e) {
                e.printStackTrace();
        }
    }
}
