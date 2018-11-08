package edu.usfca.cs.mr.wordcount.Analysis_08;

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
        extends Mapper<LongWritable, Text, Text, MusicWritable> {
    public static Map<String,Integer> blues = new HashMap<>();
    public static Map<String,Integer> classicals = new HashMap<>();

    public WordCountMapper(){
        try {
            File file = new File("./MusicKeywords/blues.txt");
            BufferedReader br = new BufferedReader(new FileReader(file));
            String mc;
            while ((mc = br.readLine()) != null)
                blues.put(mc,1);

            file = new File("./MusicKeywords/classical.txt");
            br = new BufferedReader(new FileReader(file));
            String cl;
            while ((cl = br.readLine()) != null)
                classicals.put(cl,1);

        }catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());
            String subreddit = json.get("subreddit").toString();
            String body = json.get("body").toString();

            MusicWritable rw = new MusicWritable(body);
            context.write(new Text(body), rw);

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}

