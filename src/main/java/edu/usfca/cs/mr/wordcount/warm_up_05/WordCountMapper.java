package edu.usfca.cs.mr.wordcount.warm_up_05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(value.toString());

            Long tempTimeStamp = Long.parseLong(json.get("created_utc").toString());

            LocalDateTime triggerTime =
                    LocalDateTime.ofInstant(Instant.ofEpochSecond(tempTimeStamp),
                            TimeZone.getTimeZone("UTC").toZoneId());

                context.write(new Text(triggerTime.getYear()+"_"+ triggerTime.getMonthValue()), new IntWritable(1));

        } catch (ParseException e) {
                e.printStackTrace();
        }
    }
}
