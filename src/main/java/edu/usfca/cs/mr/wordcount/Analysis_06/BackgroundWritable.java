package edu.usfca.cs.mr.wordcount.Analysis_06;

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
import static edu.usfca.cs.mr.wordcount.Analysis_06.WordCountMapper.*;


public class BackgroundWritable implements Writable {

    private LongWritable positiveCount;
    private LongWritable negativeCount;
    private Text subreddit;
    private IntWritable hour;
    private IntWritable week;
    private LongWritable totalCount;
    private LongWritable screamerCount;
    private MapWritable sportCount;
    private MapWritable hobbyCount;
    private LongWritable csCount;
    private MapWritable location;


    private String text;
    private Long tempTimeStamp;


    private List<Map.Entry<String, Long>> sortedPositiveList;
    private List<Map.Entry<String, Long>> sortedNegativeList;
    private List<Map.Entry<Integer, Long>> sortedHourList;
    private List<Map.Entry<Integer, Long>> sortedWeekList;
    private List<Map.Entry<String, Long>> sortedSportList;
    private List<Map.Entry<String, Long>> sortedHobbyList;

    private String favouriteSubreddit;
    private String unfavouriteSubreddit;


    public BackgroundWritable() {
        this.positiveCount = new LongWritable(0);
        this.negativeCount = new LongWritable(0);
        this.hour = new IntWritable(0);
        this.week = new IntWritable(0);
        this.totalCount = new LongWritable(0);
        this.screamerCount = new LongWritable(0);
        this.subreddit = new Text("");
        this.sportCount = new MapWritable();
        this.hobbyCount = new MapWritable();
        this.csCount = new LongWritable(0);
        this.location = new MapWritable();

    }

    public BackgroundWritable(List<Map.Entry<String, Long>> sortedPositiveList,
                              List<Map.Entry<String, Long>> sortedNegativeList, List<Map.Entry<Integer, Long>> sortedHourList,
                              List<Map.Entry<Integer, Long>> sortedWeekList, List<Map.Entry<String, Long>> sortedSportList,
                              List<Map.Entry<String, Long>> sortedHobbyList, LongWritable totalCount,
                              LongWritable screamerCount, LongWritable csCount ) {
        this.sortedPositiveList = sortedPositiveList;
        this.sortedNegativeList = sortedNegativeList;
        this.sortedHourList = sortedHourList;
        this.sortedWeekList = sortedWeekList;
        this.sortedSportList = sortedSportList;
        this.sortedHobbyList = sortedHobbyList;
        this.totalCount = totalCount;
        this.screamerCount = screamerCount;
        this.csCount = csCount;

    }

    public BackgroundWritable(JSONObject json) {
        this.positiveCount = new LongWritable(0);
        this.negativeCount = new LongWritable(0);
        this.hour = new IntWritable(0);
        this.week = new IntWritable(0);
        this.totalCount = new LongWritable(0);
        this.screamerCount = new LongWritable(0);
        this.subreddit = new Text("");
        this.sportCount = new MapWritable();
        this.hobbyCount = new MapWritable();
        this.csCount = new LongWritable(0);
        this.location = new MapWritable();
        this.subreddit = new Text(json.get("subreddit").toString());

        this.tempTimeStamp = Long.parseLong(json.get("created_utc").toString());
        this.text = json.get("body").toString();

//        this.words = getNumberOfWords(text);
        this.setimentAnalysisCheck();
        this.updateTime();
        this.updateScreamWords();
    }



    public void setTotal(LongWritable totalCount) {
        this.totalCount = totalCount;
    }

    public void setScreamerCount(LongWritable screamerCount) {
        this.screamerCount = screamerCount;
    }

    public void increment(BackgroundWritable sw) {
        incrementTotal(sw.totalCount.get());
        incrementScreamerCount(sw.screamerCount.get());
    }


    public void updateTime() {
        LocalDateTime triggerTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(tempTimeStamp), TimeZone.getTimeZone("UTC").toZoneId());
        hour = new IntWritable(triggerTime.getHour());
        week = new IntWritable(triggerTime.getDayOfWeek().getValue());
    }

    public LongWritable getTotal() {
        return totalCount;
    }
    public LongWritable getScreamerCount() {
        return screamerCount;
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
    public LongWritable getCsCount(){ return csCount;}

    public MapWritable getHobbyCount() {
        return hobbyCount;
    }
    public MapWritable getSportCount() {
        return sportCount;
    }

    public void setimentAnalysisCheck() {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        for (String w : word) {
            if (positive.containsKey(w))
                incrementPositiveWords();
            if (negative.containsKey(w))
                incrementNegativeWords();
            if (sport.containsKey(w))
                sportCount.put(new Text(w), new LongWritable(1));
            if (computer.containsKey(w))
                incrementComputerCount();
            if (hobby.containsKey(w))
                hobbyCount.put(new Text(w), new LongWritable(1));

        }
    }

    public void updateScreamWords(){
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        for (String w : word) {
            incrementTotal();
            boolean flag = true;
            char[] tokenSrray = w.toCharArray();
            for (char c : tokenSrray) {
                if (!(c >= 'A' && c <= 'Z')) {
                    flag = false;
                }
            }
            if (flag)
                incrementScreamerCount();
        }
    }

    private void incrementPositiveWords(long count) {
        this.positiveCount.set(this.positiveCount.get() + count);
    }
    private void incrementNegativeWords(long count) {
        this.negativeCount.set(this.negativeCount.get() + count);
    }
    private void incrementTotal(long count) {
        this.totalCount.set(this.totalCount.get() + count);
    }
    private void incrementScreamerCount(long count) {
        this.screamerCount.set(this.screamerCount.get() + count);
    }
    private void incrementComputerCount(long count) {
        this.csCount.set(this.csCount.get() + count);
    }



    public void incrementComputerCount() {
        this.incrementComputerCount(1);
    }

    public void incrementTotal() {
        this.incrementTotal(1);
    }
    public void incrementScreamerCount() {
        this.incrementScreamerCount(1);
    }
    public void incrementPositiveWords() {
        this.incrementPositiveWords(1);
    }
    public void incrementNegativeWords() {
        this.incrementNegativeWords(1);
    }

    public String finalAnalysis() {
        StringBuilder sb = new StringBuilder();
        sb.append("is a user may works on Computer Science.");
        sb.append(". He is free and prefers write comment at ");
        Map.Entry<Integer, Long> favouriteHour = sortedHourList.iterator().next();
        sb.append(favouriteHour.getKey() + " O\'clock\t");
        Map.Entry<Integer, Long> favouriteWeek = sortedHourList.iterator().next();
        sb.append(toWeekString(favouriteWeek.getKey()));

        sb.append(". He is busy and never write comment at ");
        for ( Map.Entry<Integer, Long> hour: sortedHourList){
            if (hour.getValue() == 0L){
                sb.append(hour.getKey() + " O\'clock\t");
            }
        }
        for ( Map.Entry<Integer, Long> week: sortedWeekList){
            if (week.getValue() == 0L){
                sb.append(toWeekString(week.getKey()) + "\t");
            }
        }

        if (!sortedPositiveList.isEmpty()) {
            sb.append(". His favorite Subreddit is ");
            Map.Entry<String, Long> like = sortedPositiveList.iterator().next();
            favouriteSubreddit = like.getKey();
            sb.append(favouriteSubreddit);
        }
        if (!sortedNegativeList.isEmpty()) {
            sb.append(". He don't like  ");
            Map.Entry<String, Long> dislike = sortedNegativeList.iterator().next();
            unfavouriteSubreddit = dislike.getKey();
            sb.append(unfavouriteSubreddit);
        }
        if (!sortedSportList.isEmpty()){
            sb.append(". His favourite sport is ");
            Map.Entry<String, Long> sport = sortedSportList.iterator().next();
            sb.append(sport.getKey());
        }
        if (!sortedHobbyList.isEmpty()){
            sb.append(". His favourite hobby is ");
            Map.Entry<String, Long> hobby = sortedHobbyList.iterator().next();
            sb.append(hobby.getKey());
        }
        sb.append(".\n");

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


    public void readFields(DataInput in) throws IOException {
        this.subreddit.readFields(in);
        this.positiveCount.readFields(in);
        this.negativeCount.readFields(in);
        this.hour.readFields(in);
        this.week.readFields(in);
        this.totalCount.readFields(in);
        this.screamerCount.readFields(in);
        this.sportCount.readFields(in);
        this.csCount.readFields(in);
        this.hobbyCount.readFields(in);

    }

    public void write(DataOutput out) throws IOException {
        this.subreddit.write(out);
        this.positiveCount.write(out);
        this.negativeCount.write(out);
        this.hour.write(out);
        this.week.write(out);
        this.totalCount.write(out);
        this.screamerCount.write(out);
        this.sportCount.write(out);
        this.csCount.write(out);
        this.hobbyCount.write(out);
    }


    @Override
    public String toString() {
        return "\tFinal Analysis: " +finalAnalysis();
    }

}
