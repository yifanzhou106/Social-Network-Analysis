package edu.usfca.cs.mr.wordcount.Analysis_07;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
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
        extends Reducer<Text, BackgroundWritable, Text, Text> {

    private Map<String, BackgroundWritable> userBackgroundMap = new HashMap<>();

    @Override
    protected void reduce(
            Text key, Iterable<BackgroundWritable> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Long> positiveMap = new HashMap<>();
        Map<String, Long> negativeMap = new HashMap<>();
        Map<Integer, Long> hourMap = new HashMap<>();
        Map<Integer, Long> weekMap = new HashMap<>();
        Map<String, Long> sportMap = new HashMap<>();
        Map<String, Long> hobbyMap = new HashMap<>();

        long totalWords = 0;
        long screamerCount = 0;
        long csCount = 0;

        for (int i = 0; i < 24; i++)
            hourMap.put(i, 0L);
        for (int i = 1; i < 8; i++)
            weekMap.put(i, 0L);

        for (BackgroundWritable val : values) {
            MapWritable map1 = val.getSportCount();
            MapWritable map2 = val.getHobbyCount();
            for (MapWritable.Entry entry : map1.entrySet()) {
                sportMap.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }
            for (MapWritable.Entry entry : map2.entrySet()) {
                hobbyMap.put(entry.getKey().toString(), Long.valueOf(entry.getValue().toString()));
            }

            csCount += val.getCsCount().get();
            screamerCount += val.getScreamerCount().get();
            totalWords += val.getTotal().get();

            String subreddit = val.getSubreddit().toString();
            Long positive = val.getPositiveCount().get();
            Long negative = val.getNegativeCount().get();
            int week = val.getWeek().get();
            int hour = val.getHour().get();
            screamerCount += val.getScreamerCount().get();

            /**
             * author like
             */
            if (positive > 0)
                if (!positiveMap.containsKey(subreddit)) {
                    positiveMap.put(subreddit, positive);
                } else {
                    Long temp = positiveMap.get(subreddit);
                    temp += positive;
                    positiveMap.put(subreddit, temp);
                }
            /**
             * author dislike
             */
            if (negative > 0)
                if (!negativeMap.containsKey(subreddit)) {
                    negativeMap.put(subreddit, negative);
                } else {
                    Long temp = negativeMap.get(subreddit);
                    temp += negative;
                    negativeMap.put(subreddit, temp);
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
        List<Map.Entry<String, Long>> sortedPositiveList = sortStringMap(positiveMap);
        List<Map.Entry<String, Long>> sortedNegativeList = sortStringMap(negativeMap);
        List<Map.Entry<Integer, Long>> sortedHourList = sortIntegerMap(hourMap);
        List<Map.Entry<Integer, Long>> sortedWeekList = sortIntegerMap(weekMap);
        List<Map.Entry<String, Long>> sortedSportList = sortStringMap(sportMap);
        List<Map.Entry<String, Long>> sortedHobbyList = sortStringMap(hobbyMap);

        BackgroundWritable bg = new BackgroundWritable(sortedPositiveList, sortedNegativeList,
                sortedHourList, sortedWeekList,
                sortedSportList, sortedHobbyList, new LongWritable(totalWords),
                new LongWritable(screamerCount), new LongWritable(csCount));

        userBackgroundMap.put(key.toString(), bg);
//        context.write(key, bg);

    }

    public List<Map.Entry<String, Long>> sortStringMap(Map<String, Long> map) {

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

    public List<Map.Entry<Integer, Long>> sortIntegerMap(Map<Integer, Long> map) {

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

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<String, List<String>> hobbyPair = new HashMap<>();
        Map<String, List<String>> sportPair = new HashMap<>();
        Map<String, List<String>> subredditPair = new HashMap<>();


        Set<Map.Entry<String, BackgroundWritable>> set = userBackgroundMap.entrySet();
        List<Map.Entry<String, BackgroundWritable>> list = new ArrayList<Map.Entry<String, BackgroundWritable>>(set);
        Collections.sort(list, new Comparator<Map.Entry<String, BackgroundWritable>>() {
            public int compare(Map.Entry<String, BackgroundWritable> o1,
                               Map.Entry<String, BackgroundWritable> o2) {
                if ((o2.getValue().getCsCount().get() < o1.getValue().getCsCount().get()))
                    return -1;
                else if ((o2.getValue().getCsCount().get() > o1.getValue().getCsCount().get()))
                    return 1;
                else return 0;
            }
        });

        for (int i = 0; i < 1000; i++) {
            Map.Entry<String, BackgroundWritable> entry = list.get(i);
            /**
             * update Hobby Pair
             */
            if (!hobbyPair.containsKey(entry.getValue().getFavouriteHobby())) {
                List<String> nameList = new ArrayList<>();
                nameList.add(entry.getKey());
                hobbyPair.put(entry.getValue().getFavouriteHobby(), nameList);
            } else {
                List<String> nameList = hobbyPair.get(entry.getValue().getFavouriteHobby());
                nameList.add(entry.getKey());
                hobbyPair.put(entry.getValue().getFavouriteHobby(), nameList);
            }

            /**
             * update Sport Pair
             */
            if (!sportPair.containsKey(entry.getValue().getFavouriteSport())) {
                List<String> nameList = new ArrayList<>();
                nameList.add(entry.getKey());
                sportPair.put(entry.getValue().getFavouriteSport(), nameList);
            } else {
                List<String> nameList = sportPair.get(entry.getValue().getFavouriteSport());
                nameList.add(entry.getKey());
                sportPair.put(entry.getValue().getFavouriteSport(), nameList);
            }

            /**
             * update Subreddit pair
             */
            if (!subredditPair.containsKey(entry.getValue().getFavouriteSubreddit())) {
                List<String> nameList = new ArrayList<>();
                nameList.add(entry.getKey());
                subredditPair.put(entry.getValue().getFavouriteSubreddit(), nameList);
            } else {
                List<String> nameList = subredditPair.get(entry.getValue().getFavouriteSubreddit());
                nameList.add(entry.getKey());
                subredditPair.put(entry.getValue().getFavouriteSubreddit(), nameList);
            }

//            context.write(new Text(entry.getKey()), entry.getValue());
        }
        Map<String,List<String>> realFriendPairs = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Integer> relationChance = new HashMap<>();
            Map.Entry<String, BackgroundWritable> entry = list.get(i);
            List<String> sameHobby = hobbyPair.get(entry.getValue().getFavouriteHobby());
            List<String> sameSport = sportPair.get(entry.getValue().getFavouriteSport());
            List<String> sameSubreddit = subredditPair.get(entry.getValue().getFavouriteSubreddit());
            if (sameHobby.size() > 1)
                for (String name : sameHobby) {
                    if (!name.equals(entry.getKey())) {
                        if (!relationChance.containsKey(name)) {
                            relationChance.put(name, 1);
                        } else {
                            int chance = relationChance.get(name) + 1;
                            relationChance.put(name, chance);
                        }
                    }
                }
            if (sameSport.size() > 1)
                for (String name : sameSport) {
                    if (!name.equals(entry.getKey())) {
                        if (!relationChance.containsKey(name)) {
                            relationChance.put(name, 1);
                        } else {
                            int chance = relationChance.get(name) + 1;
                            relationChance.put(name, chance);
                        }
                    }
                }
            if (sameSubreddit.size() > 1)
                for (String name : sameSubreddit) {
                    if (!name.equals(entry.getKey())) {
                        if (!relationChance.containsKey(name)) {
                            relationChance.put(name, 1);
                        } else {
                            int chance = relationChance.get(name) + 1;
                            relationChance.put(name, chance);
                        }
                    }
                }
                List<String> friendsList = new ArrayList<>();
                for (Map.Entry<String, Integer> friend:relationChance.entrySet()){
                    if (friend.getValue() >= 2){
                        friendsList.add(friend.getKey());
                    }
                }
                if (!friendsList.isEmpty())
                    realFriendPairs.put(entry.getKey(),friendsList);
        }


        for (Map.Entry<String, List<String>> entry : realFriendPairs.entrySet()) {
            context.write(new Text(entry.getKey()), new Text("\'s potential friend: "));
            for (String name: entry.getValue())
                context.write(new Text(" "), new Text(name));
        }
    }
}
