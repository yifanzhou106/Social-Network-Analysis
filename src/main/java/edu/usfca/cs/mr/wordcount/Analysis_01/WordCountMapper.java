package edu.usfca.cs.mr.wordcount.Analysis_01;

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
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, ScreamerWriter> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());
            String subreddit = json.get("subreddit").toString();
            String body = json.get("body").toString();
            StringTokenizer itr = new StringTokenizer(body);

            ScreamerWriter sw = new ScreamerWriter(subreddit);

            while (itr.hasMoreTokens()) {
                sw.incrementTotal();
                boolean flag = true;
                String token = itr.nextToken();
                char[] tokenSrray = token.toCharArray();
                for (char c : tokenSrray) {
                    if (!(c >= 'A' && c <= 'Z')) {
                        flag = false;
                    }
                }
                if (flag)
                    sw.incrementScreamerCount();
            }
            if (sw.hasCount())
                context.write(new Text(subreddit), sw);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
