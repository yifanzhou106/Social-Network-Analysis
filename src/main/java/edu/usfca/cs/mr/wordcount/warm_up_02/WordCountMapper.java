package edu.usfca.cs.mr.wordcount.warm_up_02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());
            context.write(new Text(json.get("subreddit").toString()), new LongWritable(1));

        } catch (ParseException e) {
                e.printStackTrace();
        }
    }
}
