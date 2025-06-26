import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CaseStudy4AverageTripsByDayType {

    private static final String DATASET_DIR = "/Users/mmafrar/malaysia-gtfs-history";
    private static final String OUTPUT_DIR = "/Users/mmafrar/malaysia-gtfs-history";

    // First Mapper: Process individual records
    public static class GtfsDailyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private static final Map<String, String> FEED_ID_MAP = new HashMap<>();
        static {
            FEED_ID_MAP.put("AGL", "LRT Ampang Line");
            FEED_ID_MAP.put("BRT", "BRT Sunway Line");
            FEED_ID_MAP.put("KGL", "MRT Kajang Line");
            FEED_ID_MAP.put("KJL", "LRT Kelana Jaya Line");
            FEED_ID_MAP.put("MRL", "KL Monorail Line");
            FEED_ID_MAP.put("PYL", "MRT Putrajaya Line");
            FEED_ID_MAP.put("SPL", "LRT Sri Petaling Line");
            FEED_ID_MAP.put("Prasarana_RapidBusKL", "Rapid Bus KL");
            FEED_ID_MAP.put("Prasarana_RapidBusKuantan", "Rapid Bus Kuantan");
        }

        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // Skip header
            if (key.get() == 0 && value.toString().contains("trip_id")) {
                return;
            }

            String[] fields = value.toString().split(",");
            if (fields.length < 6) return;

            try {
                // Parse fields - CORRECTED FIELD INDICES
                String feedId = fields[0];
                String subdir = fields[1];
                int headwaySecs = Integer.parseInt(fields[2]);
                String startTime = fields[3];
                String endTime = fields[4];
                String tripId = fields[6];

                // Adjust feed_id for RapidRail
                if ("Prasarana_RapidRailKL".equals(feedId)) {
                    feedId = tripId.substring(0, 3);
                }

                // Map feed_id to operator name
                String operatorName = FEED_ID_MAP.getOrDefault(feedId, feedId);

                // Determine day type (weekday/weekend)
                Date subdirDate = dateFormat.parse(subdir);
                Calendar cal = Calendar.getInstance();
                cal.setTime(subdirDate);
                int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
                String dayType = (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY) 
                                ? "weekend" : "weekday";

                // Calculate duration and number of trips
                Date start = timeFormat.parse(startTime);
                Date end = timeFormat.parse(endTime);
                long durationSecs = (end.getTime() - start.getTime()) / 1000;
                int numTrips = (int) (durationSecs / headwaySecs);

                // Emit key-value pairs for daily aggregation
                // Key: operator_name,subdir,day_type
                outputKey.set(operatorName + "," + subdir + "," + dayType);
                outputValue.set(numTrips);
                context.write(outputKey, outputValue);

            } catch (ParseException e) {
                context.getCounter("Malformed Records", "Count").increment(1);
            } catch (NumberFormatException e) {
                context.getCounter("Number Format Errors", "Count").increment(1);
            } catch (ArrayIndexOutOfBoundsException e) {
                context.getCounter("Array Index Errors", "Count").increment(1);
            }
        }
    }

    // First Reducer: Sum daily trips
    public static class GtfsDailyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Second Mapper: Prepare for daily average
    public static class AvgMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length != 2) return;

            String compositeKey = parts[0];
            int dailyTotal = Integer.parseInt(parts[1]);
            
            String[] keyParts = compositeKey.split(",");
            if (keyParts.length < 3) return;

            String operatorName = keyParts[0];
            String dayType = keyParts[2];  // [0]=operator, [1]=date, [2]=day_type
            
            outputKey.set(operatorName + "," + dayType);
            outputValue.set(dailyTotal);
            context.write(outputKey, outputValue);
        }
    }

    // Second Reducer: Calculate average daily trips
    public static class AvgReducer extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            int count = 0;

            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            int average = (int) Math.round((double) sum / count);
            result.set(String.valueOf(average));
            context.write(key, result);
        }
    }

    // Third Mapper: Prepare for pivoting
    public static class PivotMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String[] parts = value.toString().split("\t");
            if (parts.length != 2) return;

            String[] keyParts = parts[0].split(",");
            if (keyParts.length != 2) return;

            String operatorName = keyParts[0];
            String dayType = keyParts[1];
            String avgTrips = parts[1];

            outputKey.set(operatorName);
            outputValue.set(dayType + ":" + avgTrips);
            context.write(outputKey, outputValue);
        }
    }

    // Third Reducer: Pivot results
    public static class PivotReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            String weekdayAvg = "0";
            String weekendAvg = "0";

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length != 2) continue;

                if ("weekday".equals(parts[0])) {
                    weekdayAvg = parts[1];
                } else if ("weekend".equals(parts[0])) {
                    weekendAvg = parts[1];
                }
            }

            result.set("weekday_avg_daily_trips: " + weekdayAvg + ", weekend_avg_daily_trips: " + weekendAvg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Clean output directories
        if (fs.exists(new Path(OUTPUT_DIR + "/output_4"))) {
            fs.delete(new Path(OUTPUT_DIR + "/output_4"), true);
        }
        
        // Job 1: Daily aggregation
        Job job1 = Job.getInstance(conf, "GTFS Daily Aggregation");
        job1.setJarByClass(CaseStudy4AverageTripsByDayType.class);
        job1.setMapperClass(GtfsDailyMapper.class);
        job1.setReducerClass(GtfsDailyReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(DATASET_DIR + "/frequencies"));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_DIR + "/output_4/daily_totals"));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 2: Average calculation
        Job job2 = Job.getInstance(conf, "GTFS Average Calculation");
        job2.setJarByClass(CaseStudy4AverageTripsByDayType.class);
        job2.setMapperClass(AvgMapper.class);
        job2.setReducerClass(AvgReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_DIR + "/output_4/daily_totals"));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_DIR + "/output_4/averages"));

        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

        // Job 3: Pivot results
        Job job3 = Job.getInstance(conf, "GTFS Pivot Results");
        job3.setJarByClass(CaseStudy4AverageTripsByDayType.class);
        job3.setMapperClass(PivotMapper.class);
        job3.setReducerClass(PivotReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(OUTPUT_DIR + "/output_4/averages"));
        FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_DIR + "/output_4/final_results"));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

}
