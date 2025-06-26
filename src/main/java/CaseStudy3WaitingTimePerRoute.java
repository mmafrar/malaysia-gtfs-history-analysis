import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class CaseStudy3WaitingTimePerRoute {

    private static final String FEED_FILTER = "KTMB";
    private static final String DELIMITER = "!";
    private static final String BASE_DATE = "2025-01-01 ";
    private static final String DATASET_DIR = "/Users/mmafrar/malaysia-gtfs-history";
    private static final String OUTPUT_DIR = "/Users/mmafrar/malaysia-gtfs-history";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static class RoutesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("feed_id")) return;

            String[] parts = line.split(",", -1);
            if (parts.length >= 4 && FEED_FILTER.equals(parts[0].trim())) {
                String routeId = parts[3].trim();
                String routeShortName = parts[2].trim().replace(" ", "-");
                context.write(new Text(routeId), new Text("R" + DELIMITER + routeShortName));
            }
        }
    }

    public static class RoutesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    // Job 2: Process trips
    public static class TripsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("feed_id")) return;

            String[] parts = line.split(",", -1);
            if (parts.length >= 6 && FEED_FILTER.equals(parts[0].trim())) {
                String routeId = parts[4].trim();
                String tripId = parts[3].trim();
                String directionId = parts[5].trim();
                context.write(new Text(routeId), new Text("T" + DELIMITER + tripId + DELIMITER + directionId));
            }
        }
    }

    public static class TripsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            Set<String> uniqueTrips = new HashSet<>();
            for (Text val : values) {
                uniqueTrips.add(val.toString());
            }
            for (String trip : uniqueTrips) {
                context.write(key, new Text(trip));
            }
        }
    }

    // Job 3: Process stop times
    public static class StopTimesMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("feed_id")) return;

            String[] parts = line.split(",", -1);
            if (parts.length >= 5 && FEED_FILTER.equals(parts[0].trim())) {
                String tripId = parts[4].trim();
                String departureTime = parts[2].trim();
                context.write(new Text(tripId), new Text("S" + DELIMITER + departureTime));
            }
        }
    }

    public static class StopTimesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            String earliestTime = null;
            for (Text val : values) {
                String time = val.toString().substring(2);
                if (earliestTime == null || time.compareTo(earliestTime) < 0) {
                    earliestTime = time;
                }
            }
            if (earliestTime != null) {
                context.write(key, new Text("S" + DELIMITER + earliestTime));
            }
        }
    }

    // Job 4: Join routes and trips
    public static class RoutesTripsMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String routeId = parts[0];
                String valuePart = parts[1];
                context.write(new Text(routeId), new Text(valuePart));
            }
        }
    }

    public static class RoutesTripsReducer extends Reducer<Text, Text, Text, Text> {
        private List<String> routes = new ArrayList<>();
        private List<String> trips = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            routes.clear();
            trips.clear();

            for (Text val : values) {
                String valueStr = val.toString();
                if (valueStr.startsWith("R!")) {
                    routes.add(valueStr.substring(2));
                } else if (valueStr.startsWith("T!")) {
                    trips.add(valueStr.substring(2));
                }
            }

            for (String route : routes) {
                for (String trip : trips) {
                    String[] tripParts = trip.split("!");
                    if (tripParts.length >= 2) {
                        String tripId = tripParts[0];
                        String directionId = tripParts[1];
                        context.write(new Text(tripId), new Text(route + "!" + key + "!" + directionId));
                    }
                }
            }
        }
    }

    // Job 5: Join with stop times
    public static class FullJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String tripId = parts[0];
                String valuePart = parts[1];
                context.write(new Text(tripId), new Text(valuePart));
            }
        }
    }

    public static class FullJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            String joinedData = null;
            String departureTime = null;
            
            for (Text val : values) {
                String valueStr = val.toString();
                if (valueStr.startsWith("S" + DELIMITER)) {
                    departureTime = valueStr.substring(2);
                } else {
                    joinedData = valueStr;
                }
            }
            
            if (joinedData != null && departureTime != null) {
                context.write(new Text(key), new Text(joinedData + DELIMITER + departureTime));
            }
        }
    }

    // Job 6: First departure per trip
    public static class FirstDepartureMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String[] valueParts = parts[1].split(DELIMITER);
                if (valueParts.length >= 4) {
                    String routeShortName = valueParts[0];
                    String routeId = valueParts[1];
                    String directionId = valueParts[2];
                    String departureTime = valueParts[3];
                    
                    String routeDirectionId = routeId + "__" + directionId;
                    context.write(new Text(routeDirectionId), 
                                 new Text(routeShortName + "\t" + departureTime));
                }
            }
        }
    }

    // Job 7: Calculate intervals (FIXED)
    public static class IntervalMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) 
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class IntervalReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private DoubleWritable intervalOut = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            List<Long> timestamps = new ArrayList<>();
            String routeShortName = null;

            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                if (parts.length >= 2) {
                    if (routeShortName == null) {
                        routeShortName = parts[0];
                    }
                    String departureTime = parts[1];
                    
                    try {
                        String fullTime = BASE_DATE + departureTime;
                        Date date = DATE_FORMAT.parse(fullTime);
                        long timestamp = date.getTime();
                        timestamps.add(timestamp);
                    } catch (ParseException e) {
                        // Skip invalid time formats
                    }
                }
            }

            Collections.sort(timestamps);

            for (int i = 1; i < timestamps.size(); i++) {
                long diff = timestamps.get(i) - timestamps.get(i - 1);
                double intervalMinutes = diff / (1000.0 * 60.0);
                
                if (intervalMinutes > 0) {
                    intervalOut.set(intervalMinutes);
                    context.write(new Text(routeShortName), intervalOut);
                }
            }
        }
    }

    // Job 8: Calculate statistics
    public static class StatsMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context) 
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class StatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                double interval = val.get();
                if (interval < min) min = interval;
                if (interval > max) max = interval;
                sum += interval;
                count++;
            }

            if (count > 0) {
                double avg = sum / count;
                // Round to whole numbers as in PySpark
                min = Math.round(min);
                avg = Math.round(avg);
                max = Math.round(max);
                result.set(String.format("%.0f\t%.0f\t%.0f", min, avg, max));
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Clean output directories
        if (fs.exists(new Path(OUTPUT_DIR + "/output_3"))) {
            fs.delete(new Path(OUTPUT_DIR + "/output_3"), true);
        }

        // Define paths for /user/gooiyinhong/output_3/
        Path routesOutput = new Path(OUTPUT_DIR + "/output_3/routes");
        Path tripsOutput = new Path(OUTPUT_DIR + "/output_3/trips");
        Path stopTimesOutput = new Path(OUTPUT_DIR + "/output_3/stop_times");
        Path routesTripsOutput = new Path(OUTPUT_DIR + "/output_3/routes_trips");
        Path fullJoinOutput = new Path(OUTPUT_DIR + "/output_3/full_join");
        Path firstDepartureOutput = new Path(OUTPUT_DIR + "/output_3/first_departure");
        Path intervalsOutput = new Path(OUTPUT_DIR + "/output_3/intervals");
        Path statsOutput = new Path(OUTPUT_DIR + "/output_3/stats");

        // Cleanup previous runs
        if (fs.exists(routesOutput)) fs.delete(routesOutput, true);
        if (fs.exists(tripsOutput)) fs.delete(tripsOutput, true);
        if (fs.exists(stopTimesOutput)) fs.delete(stopTimesOutput, true);
        if (fs.exists(routesTripsOutput)) fs.delete(routesTripsOutput, true);
        if (fs.exists(fullJoinOutput)) fs.delete(fullJoinOutput, true);
        if (fs.exists(firstDepartureOutput)) fs.delete(firstDepartureOutput, true);
        if (fs.exists(intervalsOutput)) fs.delete(intervalsOutput, true);
        if (fs.exists(statsOutput)) fs.delete(statsOutput, true);

        // Job 1: Process routes
        Job job1 = Job.getInstance(conf, "Process Routes");
        job1.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job1.setMapperClass(RoutesMapper.class);
        job1.setReducerClass(RoutesReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(DATASET_DIR + "/routes"));
        FileOutputFormat.setOutputPath(job1, routesOutput);
        if (!job1.waitForCompletion(true)) System.exit(1);

        // Job 2: Process trips
        Job job2 = Job.getInstance(conf, "Process Trips");
        job2.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job2.setMapperClass(TripsMapper.class);
        job2.setReducerClass(TripsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(DATASET_DIR + "/trips"));
        FileOutputFormat.setOutputPath(job2, tripsOutput);
        if (!job2.waitForCompletion(true)) System.exit(1);

        // Job 3: Process stop times
        Job job3 = Job.getInstance(conf, "Process Stop Times");
        job3.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job3.setMapperClass(StopTimesMapper.class);
        job3.setReducerClass(StopTimesReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(DATASET_DIR + "/stop_times"));
        FileOutputFormat.setOutputPath(job3, stopTimesOutput);
        if (!job3.waitForCompletion(true)) System.exit(1);

        // Job 4: Join routes and trips
        Job job4 = Job.getInstance(conf, "Join Routes and Trips");
        job4.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job4.setMapperClass(RoutesTripsMapper.class);
        job4.setReducerClass(RoutesTripsReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job4, routesOutput);
        FileInputFormat.addInputPath(job4, tripsOutput);
        FileOutputFormat.setOutputPath(job4, routesTripsOutput);
        if (!job4.waitForCompletion(true)) System.exit(1);

        // Job 5: Full join with stop times
        Job job5 = Job.getInstance(conf, "Full Join");
        job5.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job5.setMapperClass(FullJoinMapper.class);
        job5.setReducerClass(FullJoinReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5, routesTripsOutput);
        FileInputFormat.addInputPath(job5, stopTimesOutput); // Fixed variable name
        FileOutputFormat.setOutputPath(job5, fullJoinOutput);
        if (!job5.waitForCompletion(true)) System.exit(1);

        // Job 6: First departure per trip
        Job job6 = Job.getInstance(conf, "First Departure");
        job6.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job6.setMapperClass(FirstDepartureMapper.class);
        job6.setNumReduceTasks(0); // Map-only job
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job6, fullJoinOutput);
        FileOutputFormat.setOutputPath(job6, firstDepartureOutput);
        if (!job6.waitForCompletion(true)) System.exit(1);

        // Job 7: Calculate intervals
        Job job7 = Job.getInstance(conf, "Calculate Intervals");
        job7.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job7.setInputFormatClass(KeyValueTextInputFormat.class);
        job7.setMapperClass(IntervalMapper.class);
        job7.setReducerClass(IntervalReducer.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(Text.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(DoubleWritable.class);
        job7.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job7, firstDepartureOutput);
        FileOutputFormat.setOutputPath(job7, intervalsOutput);
        if (!job7.waitForCompletion(true)) System.exit(1);

        // Job 8: Calculate statistics
        Job job8 = Job.getInstance(conf, "Calculate Statistics");
        job8.setJarByClass(CaseStudy3WaitingTimePerRoute.class);
        job8.setInputFormatClass(SequenceFileInputFormat.class);
        job8.setMapperClass(StatsMapper.class);
        job8.setReducerClass(StatsReducer.class);
        job8.setMapOutputKeyClass(Text.class);
        job8.setMapOutputValueClass(DoubleWritable.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job8, intervalsOutput);
        FileOutputFormat.setOutputPath(job8, statsOutput);
        System.exit(job8.waitForCompletion(true) ? 0 : 1);
    }

}
