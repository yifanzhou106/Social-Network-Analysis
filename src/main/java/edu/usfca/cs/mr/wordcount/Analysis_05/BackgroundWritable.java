package edu.usfca.cs.mr.wordcount.Analysis_05;

import org.apache.hadoop.io.*;
import org.json.simple.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public class BackgroundWritable implements Writable {

    private LongWritable positiveCount;
    private LongWritable negativeCount;
    private Text subreddit;
    private IntWritable hour;
    private IntWritable week;

    private MapWritable location = new MapWritable();


    private String text;
    private Long tempTimeStamp;


    private Map<String, Integer> positiveSample;
    private Map<String, Integer> negativeSample;

    private List<Map.Entry<String, Long>> sortedPositiveList;
    private List<Map.Entry<String, Long>> sortedNegativeList;
    private List<Map.Entry<String, Long>> sortedScoreList;

    private List<Map.Entry<Integer, Long>> sortedHourList;
    private List<Map.Entry<Integer, Long>> sortedWeekList;

    private Map<Integer,Long> hourMap = new HashMap<>();
    private Map<Integer,Long> weekMap = new HashMap<>();

    private String favouriteSubreddit;
    private String unfavouriteSubreddit;

    private Long totalCommentNumber;


    public BackgroundWritable() {
        this.positiveCount = new LongWritable(0);
        this.negativeCount = new LongWritable(0);
        this.hour = new IntWritable(0);
        this.week = new IntWritable(0);
        this.subreddit = new Text("");
    }

//    public BackgroundWritable(List<Map.Entry<String, Long>> sortedPositiveList, List<Map.Entry<String, Long>> sortedNegativeList, List<Map.Entry<Integer, Long>> sortedHourList, List<Map.Entry<Integer, Long>> sortedWeekList) {
//        this.sortedPositiveList = sortedPositiveList;
//        this.sortedNegativeList = sortedNegativeList;
//        this.sortedHourList = sortedHourList;
//        this.sortedWeekList = sortedWeekList;
//
//    }

//    public BackgroundWritable(List<Map.Entry<String, Long>> sortedPositiveList, List<Map.Entry<String, Long>> sortedNegativeList,Map<Integer, Long> hourMap, Map<Integer, Long> weekMap) {
//        this.sortedPositiveList = sortedPositiveList;
//        this.sortedNegativeList = sortedNegativeList;
//        this.hourMap = hourMap;
//        this.weekMap = weekMap;
//
//    }

    public BackgroundWritable(List<Map.Entry<String, Long>> sortedScoreList, Map<Integer, Long> hourMap, Map<Integer, Long> weekMap, Long totalCommentNumber) {
        this.sortedScoreList = sortedScoreList;
        this.hourMap = hourMap;
        this.weekMap = weekMap;
        this.totalCommentNumber = totalCommentNumber;

    }

    public BackgroundWritable(JSONObject json, Map<String, Integer> positiveSample, Map<String, Integer> negativeSample) {
        this.positiveCount = new LongWritable(0);
        this.negativeCount = new LongWritable(0);
        this.subreddit = new Text(json.get("subreddit").toString());
        this.hour = new IntWritable(0);
        this.week = new IntWritable(0);

        this.tempTimeStamp = Long.parseLong(json.get("created_utc").toString());
        this.text = json.get("body").toString();
        this.positiveSample = positiveSample;
        this.negativeSample = negativeSample;

//        this.words = getNumberOfWords(text);
        this.setimentAnalysisCheck();
        this.updateTime();
    }


    public void updateTime() {
        LocalDateTime triggerTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(tempTimeStamp), TimeZone.getTimeZone("UTC").toZoneId());
        hour = new IntWritable(triggerTime.getHour());
        week = new IntWritable(triggerTime.getDayOfWeek().getValue());
    }


    public Text getSubreddit() {
        return subreddit;
    }

    public IntWritable getHour() {
        return hour;
    }

    public IntWritable getWeek() {
        return week;
    }


    public LongWritable getNegativeCount() {
        return negativeCount;
    }

    public LongWritable getPositiveCount() {
        return positiveCount;
    }

    public void setimentAnalysisCheck() {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        for (String w : word) {
            if (positiveSample.containsKey(w))
                incrementPositiveWords();
            else if (negativeSample.containsKey(w))
                incrementNegativeWords();
        }
    }


    private void incrementPositiveWords(long count) {
        this.positiveCount.set(this.positiveCount.get() + count);
    }

    private void incrementNegativeWords(long count) {
        this.negativeCount.set(this.negativeCount.get() + count);
    }


    public void incrementPositiveWords() {
        this.incrementPositiveWords(1);
    }

    public void incrementNegativeWords() {
        this.incrementNegativeWords(1);
    }

    public String finalAnalysis() {
        StringBuilder sb = new StringBuilder();
        long count = sortedScoreList.size();
        if (count > 3)
            count = 3;
        sb.append("\nTop 3 favourite Subreddit are ");
        for (int i = 0; i< count; i++){
            Map.Entry<String, Long> like = sortedScoreList.get(i);
            favouriteSubreddit = like.getKey();
            sb.append(favouriteSubreddit + ", ");
        }

        sb.append("\nTop 1 hate Subreddit are ");
        for (int i = sortedScoreList.size()-1; i > sortedScoreList.size() - 2; i--){
            Map.Entry<String, Long> dislike = sortedScoreList.get(i);
            unfavouriteSubreddit = dislike.getKey();
            sb.append(unfavouriteSubreddit + ", ");
        }
        sb.append("\nAuthor prefers write comment at ");
//        Map.Entry<Integer, Long> favouriteHour = sortedHourList.iterator().next();
//        sb.append(favouriteHour.getKey() + " O\'clock");

        sb.append(hourMap.toString());

//
        sb.append("\nAuthor prefers write comment at day of week: ");
//        Map.Entry<Integer, Long> favouriteWeek = sortedHourList.iterator().next();
//        sb.append(toWeekString(favouriteWeek.getKey()));
        sb.append(weekMap.toString());


//        sb.append("\nAuthor never write comment at ");
//        for ( Map.Entry<Integer, Long> hour: sortedHourList){
//            if (hour.getValue() == 0L){
//                sb.append(hour.getKey() + " O\'clock\t");
//            }
//        }
//        sb.append("\nAuthor never write comment at day of week: ");
//        for ( Map.Entry<Integer, Long> week: sortedWeekList){
//            if (week.getValue() == 0L){
//                sb.append(toWeekString(week.getKey()) + "\t");
//            }
//        }

        return sb.toString();
    }

    private String toWeekString(int num){
        String week = "";
        switch (num){
            case 1:  week = ("Monday");
                break;
            case 2:  week = ("Tuesday");
                break;
            case 3:  week = ("Wednesday");
                break;
            case 4:  week = ("Thursday");
                break;
            case 5:  week = ("Friday");
                break;
            case 6:  week = ("Saturday");
                break;
            case 7:  week = ("Sunday");
                break;
        }
        return week;
    }

    private String cleanLine(String line) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c < 128 && Character.isLetter(c)) {
                buffer.append(c);
            } else {
                buffer.append(' ');
            }
        }
        return buffer.toString().toLowerCase();
    }

    private double round(double d, int decimalPlace) {
        // see the Javadoc about why we use a String in the constructor
        // http://java.sun.com/j2se/1.5.0/docs/api/java/math/BigDecimal.html#BigDecimal(double)
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }
    public boolean hasCount() {
        return this.positiveCount.get() > 0
                || this.negativeCount.get() > 0
                && (this.week.get() >= 0 && this.hour.get()>=0);
    }

    public void readFields(DataInput in) throws IOException {
        this.subreddit.readFields(in);
        this.positiveCount.readFields(in);
        this.negativeCount.readFields(in);
        this.hour.readFields(in);
        this.week.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        this.subreddit.write(out);
        this.positiveCount.write(out);
        this.negativeCount.write(out);
        this.hour.write(out);
        this.week.write(out);
    }


    @Override
    public String toString() {
        return "\tFinal Analysis: " +finalAnalysis();
    }

}
