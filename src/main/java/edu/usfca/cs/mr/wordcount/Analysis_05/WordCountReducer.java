package edu.usfca.cs.mr.wordcount.Analysis_05;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class WordCountReducer
        extends Reducer<Text, BackgroundWritable, Text, BackgroundWritable> {


    @Override
    protected void reduce(
            Text key, Iterable<BackgroundWritable> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Long> positiveMap = new HashMap<>();
        Map<String, Long> negativeMap = new HashMap<>();
        Map<String, Long> scoreMap = new HashMap<>();

        Map<Integer,Long> hourMap = new HashMap<>();
        Map<Integer,Long> weekMap = new HashMap<>();
        Long totalCommentNumber = 0L;

        for (int i = 0; i<24; i++)
            hourMap.put(i,0L);
        for (int i = 1; i<8; i++)
            weekMap.put(i,0L);

        for (BackgroundWritable val : values) {
            totalCommentNumber++;
            String subreddit = val.getSubreddit().toString();
            Long positive = val.getPositiveCount().get();
            Long negative = val.getNegativeCount().get();
            int week = val.getWeek().get();
            int hour = val.getHour().get();

            /**
             * author like
             */
            if (positive > 0 || negative > 0) {
                Long score = positive - negative;
                if (!scoreMap.containsKey(subreddit)) {
                    scoreMap.put(subreddit, score);
                } else {
                    Long temp = scoreMap.get(subreddit);
                    temp += score;
                    scoreMap.put(subreddit, temp);
                }
            }

            /**
             * author active day of week
             */
            if (!weekMap.containsKey(week)) {
                weekMap.put(week, 1L);
            } else {
                Long temp = weekMap.get(week);
                temp += 1L;
                weekMap.put(week, temp);
            }

            /**
             * author active hour
             */
            if (!hourMap.containsKey(hour)) {
                hourMap.put(hour, 1L);
            } else {
                Long temp = hourMap.get(hour);
                temp += 1L;
                hourMap.put(hour, temp);
            }

        }
//        List<Map.Entry<String, Long>> sortedPositiveList = sortStringMap(positiveMap);
//        List<Map.Entry<String, Long>> sortedNegativeList = sortStringMap(negativeMap);
//        List<Map.Entry<Integer, Long>> sortedHourList = sortIntegerMap(hourMap);
//        List<Map.Entry<Integer, Long>> sortedWeekList = sortIntegerMap(weekMap);
        List<Map.Entry<String, Long>> sortedScoreList = sortStringMap(scoreMap);
//        BackgroundWritable bg = new BackgroundWritable(sortedPositiveList,sortedNegativeList,hourMap,weekMap);
        BackgroundWritable bg = new BackgroundWritable(sortedScoreList,hourMap,weekMap, totalCommentNumber);
        if (totalCommentNumber >2000L)
            context.write(key, bg);

    }

    public List<Map.Entry<String, Long>> sortStringMap (Map<String,Long> map){

        Set<Map.Entry<String, Long>> set = map.entrySet();
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1,
                               Map.Entry<String, Long> o2) {
                if ((o2.getValue() < o1.getValue()))
                    return -1;
                else if ((o2.getValue() > o1.getValue()))
                    return 1;
                else return 0;
            }
        });
        return list;
    }

    public List<Map.Entry<Integer, Long>> sortIntegerMap (Map<Integer,Long> map){

        Set<Map.Entry<Integer, Long>> set = map.entrySet();
        List<Map.Entry<Integer, Long>> list = new ArrayList<Map.Entry<Integer, Long>>(set);
        Collections.sort(list, new Comparator<Map.Entry<Integer, Long>>() {
            public int compare(Map.Entry<Integer, Long> o1,
                               Map.Entry<Integer, Long> o2) {
                if ((o2.getValue() < o1.getValue()))
                    return -1;
                else if ((o2.getValue() > o1.getValue()))
                    return 1;
                else return 0;
            }
        });
        return list;
    }
//
//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//
//        Set<Map.Entry<String, Long>> set = positiveMap.entrySet();
//        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(set);
//        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
//            public int compare(Map.Entry<String, Long> o1,
//                               Map.Entry<String, Long> o2) {
//                if ((o2.getValue() < o1.getValue()))
//                    return -1;
//                else if ((o2.getValue() > o1.getValue()))
//                    return 1;
//                else return 0;
//            }
//        });
//        context.write(new Text("***************Positive Top "), new LongWritable(5));
//        for (int i = 0; i < 5; i++) {
//            Map.Entry<String, Long> entry = list.get(i);
//            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
//        }
////        for (Map.Entry<String, Long> entry : list) {
////            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
////        }
//
//        set = negativeMap.entrySet();
//        list = new ArrayList<Map.Entry<String, Long>>(set);
//        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
//            public int compare(Map.Entry<String, Long> o1,
//                               Map.Entry<String, Long> o2) {
//                if ((o2.getValue() < o1.getValue()))
//                    return -1;
//                else if ((o2.getValue() > o1.getValue()))
//                    return 1;
//                else return 0;
//            }
//        });
//        context.write(new Text("\n\n***************Negative Top "), new LongWritable(5));
//        for (int i = 0; i < 5; i++) {
//            Map.Entry<String, Long> entry = list.get(i);
//            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
//        }
//
////        for (Map.Entry<String, Long> entry : list) {
////            context.write(new Text(entry.getKey()), new LongWritable(entry.getValue()));
////        }
//
//    }
}
