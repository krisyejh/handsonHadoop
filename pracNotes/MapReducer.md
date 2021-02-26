

#### Run Locally

```
python HadoopMaterials/RatingsBreakdown-ml-latest-small.py ml-latest-small/ratings.csv
```







#### Run   in HadoopCluster with file on Hadoop

```
python HadoopMaterials/RatingsBreakdown-ml-latest-small.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/raj_ops/ml-25m/ratings.csv
```



Outputs are: 

```
No configs found; falling back on auto-configuration
No configs specified for hadoop runner
Looking for hadoop binary in $PATH...
Found hadoop binary: /usr/bin/hadoop
Using Hadoop version 2.7.3.2.6.5.0
Creating temp directory /tmp/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571
uploading working dir files to hdfs:///user/raj_ops/tmp/mrjob/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571/files/wd...
Copying other local files to hdfs:///user/raj_ops/tmp/mrjob/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571/files/
Running step 1 of 1...
  packageJobJar: [] [/usr/hdp/2.6.5.0-292/hadoop-mapreduce/hadoop-streaming-2.7.3.2.6.5.0-292.jar] /tmp/streamjob4459720946465822102.jar tmpDir=null
  Connecting to ResourceManager at sandbox-hdp.hortonworks.com/172.18.0.2:8032
  Connecting to Application History server at sandbox-hdp.hortonworks.com/172.18.0.2:10200
  Connecting to ResourceManager at sandbox-hdp.hortonworks.com/172.18.0.2:8032
  Connecting to Application History server at sandbox-hdp.hortonworks.com/172.18.0.2:10200
  Total input paths to process : 1
  Adding a new node: /default-rack/172.18.0.2:50010
  number of splits:5
  Submitting tokens for job: job_1606904446182_0003
  Submitted application application_1606904446182_0003
  The url to track the job: http://sandbox-hdp.hortonworks.com:8088/proxy/application_1606904446182_0003/
  Running job: job_1606904446182_0003
  Job job_1606904446182_0003 running in uber mode : false
   map 0% reduce 0%
   map 3% reduce 0%
   map 6% reduce 0%
   map 8% reduce 0%
   map 10% reduce 0%
   map 13% reduce 0%
   map 15% reduce 0%
   map 17% reduce 0%
   map 20% reduce 0%
   map 22% reduce 0%
   map 23% reduce 0%
   map 24% reduce 0%
   map 25% reduce 0%
   map 27% reduce 0%
   map 28% reduce 0%
   map 29% reduce 0%
   map 30% reduce 0%
   map 32% reduce 0%
   map 34% reduce 0%
   map 35% reduce 0%
   map 36% reduce 0%
   map 37% reduce 0%
   map 38% reduce 0%
   map 40% reduce 0%
   map 42% reduce 0%
   map 44% reduce 0%
   map 46% reduce 0%
   map 47% reduce 0%
   map 48% reduce 0%
   map 49% reduce 0%
   map 51% reduce 0%
   map 52% reduce 0%
   map 53% reduce 0%
   map 54% reduce 0%
   map 56% reduce 0%
   map 58% reduce 0%
   map 60% reduce 0%
   map 61% reduce 0%
   map 63% reduce 0%
   map 66% reduce 0%
   map 72% reduce 0%
   map 76% reduce 0%
   map 87% reduce 0%
   map 93% reduce 0%
   map 99% reduce 0%
   map 100% reduce 0%
   map 100% reduce 68%
   map 100% reduce 69%
   map 100% reduce 71%
   map 100% reduce 73%
   map 100% reduce 79%
   map 100% reduce 83%
   map 100% reduce 92%
   map 100% reduce 95%
   map 100% reduce 100%
  Job job_1606904446182_0003 completed successfully
  Output directory: hdfs:///user/raj_ops/tmp/mrjob/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571/output
Counters: 49
	File Input Format Counters
		Bytes Read=678785275
	File Output Format Counters
		Bytes Written=148
	File System Counters
		FILE: Number of bytes read=500002046
		FILE: Number of bytes written=750949706
		FILE: Number of large read operations=0
		FILE: Number of read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=678785890
		HDFS: Number of bytes written=148
		HDFS: Number of large read operations=0
		HDFS: Number of read operations=18
		HDFS: Number of write operations=2
	Job Counters
		Data-local map tasks=5
		Launched map tasks=5
		Launched reduce tasks=1
		Total megabyte-milliseconds taken by all map tasks=147966500
		Total megabyte-milliseconds taken by all reduce tasks=18042250
		Total time spent by all map tasks (ms)=591866
		Total time spent by all maps in occupied slots (ms)=591866
		Total time spent by all reduce tasks (ms)=72169
		Total time spent by all reduces in occupied slots (ms)=72169
		Total vcore-milliseconds taken by all map tasks=591866
		Total vcore-milliseconds taken by all reduce tasks=72169
	Map-Reduce Framework
		CPU time spent (ms)=398830
		Combine input records=0
		Combine output records=0
		Failed Shuffles=0
		GC time elapsed (ms)=6573
		Input split bytes=615
		Map input records=25000096
		Map output bytes=200000771
		Map output materialized bytes=250000993
		Map output records=25000096
		Merged Map outputs=5
		Physical memory (bytes) snapshot=1148665856
		Reduce input groups=11
		Reduce input records=25000096
		Reduce output records=11
		Reduce shuffle bytes=250000993
		Shuffled Maps =5
		Spilled Records=75000288
		Total committed heap usage (bytes)=652214272
		Virtual memory (bytes) snapshot=11673128960
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
job output is in hdfs:///user/raj_ops/tmp/mrjob/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571/output
Streaming final output from hdfs:///user/raj_ops/tmp/mrjob/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571/output...
"0.5"	393068
"1.0"	776815
"1.5"	399490
"2.0"	1640868
"2.5"	1262797
"3.0"	4896928
"3.5"	3177318
"4.0"	6639798
"4.5"	2200539
"5.0"	3612474
"rating"	1
Removing HDFS temp directory hdfs:///user/raj_ops/tmp/mrjob/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571...
Removing temp directory /tmp/RatingsBreakdown-ml-latest-small.raj_ops.20210210.064153.140571...
```







### Top Movies 

#### Run Locally

```
python TopMovies.py ml-100k/u.data
```





```
python TopMovies.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/ml-100k/u.data
```

Results are:

```
...
...
"98"	"00390"
"7"	"00392"
"56"	"00394"
"127"	"00413"
"174"	"00420"
"121"	"00429"
"300"	"00431"
"1"	"00452"
"288"	"00478"
"286"	"00481"
"294"	"00485"
"181"	"00507"
"100"	"00508"
"258"	"00509"
"50"	"00584"
Removing HDFS temp directory hdfs:///user/raj_ops/tmp/mrjob/TopMovies.raj_ops.20210210.070927.670209...
Removing temp directory /tmp/TopMovies.raj_ops.20210210.070927.670209...
```





#### Run other dataset

```
python TopMovies-csv.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/raj_ops/ml-25m/ratings.csv
```



outputs are:

```
No configs found; falling back on auto-configuration
No configs specified for hadoop runner
Looking for hadoop binary in $PATH...
Found hadoop binary: /usr/bin/hadoop
Using Hadoop version 2.7.3.2.6.5.0
Creating temp directory /tmp/TopMovies-csv.raj_ops.20210210.071246.828126
uploading working dir files to hdfs:///user/raj_ops/tmp/mrjob/TopMovies-csv.raj_ops.20210210.071246.828126/files/wd...
	Copying other local files to hdfs:///user/raj_ops/tmp/mrjob/TopMovies-csv.raj_ops.20210210.071246.828126/files/
Running step 1 of 2...

...
...

"858"	"52498"
"2858"	"53689"
"1198"	"54675"
"1210"	"54917"
"50"	"55366"
"4993"	"55736"
"1"	"57309"
"1196"	"57361"
"589"	"57379"
"2959"	"58773"
"110"	"59184"
"527"	"60411"
"480"	"64144"
"260"	"68717"
"2571"	"72674"
"593"	"74127"
"296"	"79672"
"318"	"81482"
"356"	"81491"
Removing HDFS temp directory hdfs:///user/raj_ops/tmp/mrjob/TopMovies-csv.raj_ops.20210210.071246.828126...
Removing temp directory /tmp/TopMovies-csv.raj_ops.20210210.071246.828126...
```



