package edu.usfca.cs.mr.wordcount.warm_up_04;

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
        extends Mapper<LongWritable, Text, Text, Text> {
    private int count =0;
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
            Long tempTimeStamp = Long.parseLong(json.get("created_utc").toString());
//            Timestamp ts = new Timestamp(tempTimeStamp);
//            Calendar cal = Calendar.getInstance();
//            cal.setTime(new Date(tempTimeStamp*1000));
//            int date = cal.get(Calendar.DATE);
//            int month = cal.get(Calendar.MONTH);
//            int year = cal.get(Calendar.YEAR);

            LocalDateTime triggerTime =
                    LocalDateTime.ofInstant(Instant.ofEpochSecond(tempTimeStamp),
                            TimeZone.getTimeZone("UTC").toZoneId());
            int date = triggerTime.getDayOfMonth();
            int month = triggerTime.getMonthValue();
            int year = triggerTime.getYear();
            String birthday = "Date: " + month + ". " + date + ". "+year;

            if (date == 3 && month == 8 && count >= 20) {
                context.write(new Text(json.get("body").toString()), new Text(birthday));
                count=0;
            }
            count++;

        } catch (ParseException e) {
                e.printStackTrace();
        }
    }
}
