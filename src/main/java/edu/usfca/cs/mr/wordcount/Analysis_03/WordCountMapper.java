package edu.usfca.cs.mr.wordcount.Analysis_03;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.StringTokenizer;




/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 * Search "WebOriginal"
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, ReadableWriter> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());
            String subreddit = json.get("subreddit").toString();
            String body = json.get("body").toString();

            ReadableWriter rw = new ReadableWriter(body);
            context.write(new Text(body), rw);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}

