package edu.usfca.cs.mr.wordcount.Analysis_07;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 * Search "WebOriginal"
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text,BackgroundWritable> {

    public static Map<String,Integer> positive = new HashMap<>();
    public static Map<String,Integer> negative = new HashMap<>();
    public static Map<String,Integer> sport = new HashMap<>();
    public static Map<String,Integer> computer = new HashMap<>();
    public static Map<String,Integer> hobby = new HashMap<>();


    public WordCountMapper(){
        ImportHobby hl = new ImportHobby();
        ImportSport sl = new ImportSport();
        ImportCS csl = new ImportCS();
        ImportPosNeg pnl = new ImportPosNeg();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());
            String author = json.get("author").toString();


            BackgroundWritable bg = new BackgroundWritable(json);
            if (!author.equals(""))
              context.write(new Text(author), bg);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}

