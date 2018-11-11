#Big Data

## Warm-up
1. How many records are in the dataset?

Total line: 	2661983402.

This question is quite straightforward. We just map every lines in "Total line". 

2. How many unique subreddits are there?
 
Unique Subreddit is 417835. I mapped every subreddit <"subreddit", 1> and in the reducer, increase the unique count if the count of this subreddit is 1. 
   
3. What user wrote the most comments in July of 2012? What was the user’s top three most-upvoted comments?

a. qkme_transcriber has 18098 comments which wrote the most comments. I mapped every author <"author", 1>.

b. What was the user’s top three most-upvoted comments?
That sounds like a tenderloin. 	6
&gt; once a couple becomes married all of their assets are joined in the eyes of the law.

This sounds ridiculous and barely any better than the stone age to me. At least the man isn't owning the woman so that's progress?

It runs counter to all of our values as a society. What happened to individualism?	6
The pic on the left totally looks like a girl that would be fun to be friends with. 	6

I mapped every body <"body", ups>. and create a sorted map in the reducer. Print top 3 ups comments in the clean up.

4.Choose a day of significance to you (e.g., your birthday), and retrieve a 5% sample of the comments posted on this particular day across all 5 years of the dataset.

I use LocalDateTime library to transfer the unix timestamp to date, month and year. For retrieving a 5% sample, I have a count start from 0, once the timestamp == my birthday, count++. 
If this count become 95, then write<body, my birthday>, count becomes 0. It can approximately get 5% data.

5. The number of comments posted per year will likely trend upward over time as more users join Reddit. Use feature scaling to normalize the number of comments per month from 0.0 to 1.0 and plot the values for each year. This way, we can isolate the proportion of comments across months. Do you notice any patterns?

##Analysis
1. Write a job to find users that scream a lot, and provide a screamer score (a highly-technical metric expressed as the percentage of uppercase letters used in their comments).

a. JohnnyTsunami	Total token count:1	CAP token count:1	CAP / Total = 100.0% 

bilalisahomosexual	Total token count:10	CAP token count:10	CAP / Total = 100.0%

DIVA	Total token count:1	CAP token count:1	CAP / Total = 100.0%

CSSTesting2015	Total token count:4	CAP token count:4	CAP / Total = 100.0%

Fatima	Total token count:2	CAP token count:2	CAP / Total = 100.0%

b. qkme_transcriber	Total token count:1387757	CAP token count:207703	CAP / Total = 14.966813354211148%

narwal_bot	Total token count:1007601	CAP token count:30261	CAP / Total = 3.0032721285508845%

zaptal_47	Total token count:50838	CAP token count:17296	CAP / Total = 34.02179472048468%

tweet_poster	Total token count:337479	CAP token count:9108	CAP / Total = 2.698834594152525%

2. Readability: write a job that computes Gunning Fog Index and Flesch-Kincaid Readability (both reading ease and grade level) of user comments.

ohdang	sentenceCount:66	wordsCount: 66	syllabifyCount: 201960	complexCount: 66
GunningFog: 40.400000000000006
FleschKincaidGradeLevel: -3.3999999999999986
FleschReadingEase: -258670.18
Skyflower	sentenceCount:7	wordsCount: 7	syllabifyCount: 20382	complexCount: 7
GunningFog: 40.400000000000006
FleschKincaidGradeLevel: -3.3999999999999986
FleschReadingEase: -246125.20857142855
JonnyGFlea	sentenceCount:1	wordsCount: 1	syllabifyCount: 1798	complexCount: 1
GunningFog: 40.400000000000006
FleschKincaidGradeLevel: -3.3999999999999986
FleschReadingEase: -151904.97999999998

3.Key Terms: calculate the TF-IDF for a given subreddit.

4.Toxicity: using Sentiment Analysis, determine the top 5 positive subreddits and top 5 negative subreddits based on comment sentiment.


Matchmaker: while you work on your hit movie script, you need to pay the bills. Use your analysis skills to match up users with similar interests so that they can find love or friendship. If your algorithm is effective, you might just be able to pay rent this month!
Note: remember to explain your methodology in your report.

For this question, I am trying to help CS major persons to find their potential friends. 
I import 5 types of word lists: CS Major, Hobby, Sport, Positive, Negative. 
CS Major: Help to filter those who may work on the computer science. 
Positive: Negative: Help to find out authors' favourite subreddit. 
Hobby, Sport: Help to find out authors' favourite hobby and sport.

In this project, I will filter CS person who has more than 1000 comments and match those who have two or more from the same favourite subreddit, hobby, sport,
 and print its potential friend list.
 For example: 
  
wonderfuldog	's potential friend: 
, 	skeletor100
, 	Tailszefox
, 	karmasters
, 	Noveacc
, 	Airazz
, 	nubbinator
, 	ProudLikeCowz
, 	Leisureguy
, 	RobotAnna
, 	GenJonesMom
, 	MarinePrincePrime
, 	silverhydra
, 	zahlman
, 	Dichotomy01
, 	Maxxters
, 	spermracewinner
, 	GhostedAccount
, 	jhudsui
, 	iam4real
, 	TheFurryChef
, 	TheBlackHive
, 	yep45
, 	pissflap
, 	Tactful
, 	WarPhalange
, 	RosieLalala
, 	ramp_tram
, 	eaturbrainz
, 	Bornhuetter
, 	justanothercommenter
, 	argv_minus_one
, 	TracyMorganFreeman
, 	OJSlaughter
, 	Organs
, 	wayndom
, 	mavriksfan11
, 	Ambiwlans
, 	s73v3r
, 	BritishHobo
	
 
qkme_transcriber	's potential friend: 
, 	Aspel
, 	NukeThePope
	
If we add more conditions for matching friends, the result will be much better.