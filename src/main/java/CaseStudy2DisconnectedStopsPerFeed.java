import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashSet;

import java.util.Set;

public class CaseStudy2DisconnectedStopsPerFeed {

    private static final String DATASET_DIR = "/Users/mmafrar/malaysia-gtfs-history";
    private static final String OUTPUT_DIR = "/Users/mmafrar/malaysia-gtfs-history";

    // Round to 2 decimal places matching PySpark's behavior
    private static String roundCoordinate(String value) {
        try {
            BigDecimal bd = new BigDecimal(value);
            bd = bd.setScale(2, RoundingMode.HALF_UP);
            return bd.toString();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // Job 1: Process data and prepare for feed-specific deduplication
    public static class ProcessMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("feed_id")) return;

            String[] fields = line.split(",", -1);
            if (fields.length >= 6) {
                String feedId = fields[0].trim();
                String lat = fields[3].trim();
                String lon = fields[5].trim();
                
                if (!lat.isEmpty() && !lon.isEmpty()) {
                    try {
                        // Parse to validate but use original for deduplication
                        Double.parseDouble(lat);
                        Double.parseDouble(lon);
                        
                        // Emit feedId as key and lat,lon as value
                        context.write(new Text(feedId), new Text(lat + "," + lon));
                    } catch (NumberFormatException e) {
                        // Skip invalid coordinates
                    }
                }
            }
        }
    }

    public static class ProcessReducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text feedId, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            // Deduplicate exact coordinates within feed
            Set<String> uniqueCoords = new HashSet<>();
            for (Text val : values) {
                uniqueCoords.add(val.toString());
            }
            
            // Emit each unique coordinate for this feed
            for (String coord : uniqueCoords) {
                context.write(NullWritable.get(), new Text(feedId + "," + coord));
            }
        }
    }

    // Job 2: Round coordinates and filter to unique per feed
    public static class RoundFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split(",", 2);
            if (parts.length == 2) {
                String feedId = parts[0];
                String[] coords = parts[1].split(",", 2);
                if (coords.length == 2) {
                    String lat = roundCoordinate(coords[0]);
                    String lon = roundCoordinate(coords[1]);
                    
                    if (lat != null && lon != null) {
                        // Emit rounded coordinates with feedId
                        context.write(new Text(feedId + "," + lat + "," + lon), new Text("1"));
                    }
                }
            }
        }
    }

    public static class RoundFilterReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            int count = 0;
            for (Text val : values) {
                count++;
            }
            
            // Only emit if there's exactly one occurrence of this rounded coordinate in this feed
            if (count == 1) {
                context.write(key, NullWritable.get());
            }
        }
    }

    // Job 3: Count unique stops per feed
    public static class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 1) {
                context.write(new Text(parts[0]), one);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Job 4: Filter out coordinates that appear in multiple feeds
    public static class CrossFeedFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split(",");
            if (parts.length >= 3) {
                String feedId = parts[0];
                String lat = parts[1];
                String lon = parts[2];
                context.write(new Text(lat + "," + lon), new Text(feedId));
            }
        }
    }

    public static class CrossFeedFilterReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            Set<String> feeds = new HashSet<>();
            for (Text val : values) {
                feeds.add(val.toString());
                if (feeds.size() > 1) {
                    return; // Skip if coordinate appears in multiple feeds
                }
            }
            
            // Only emit if coordinate appears in exactly one feed
            context.write(new Text(feeds.iterator().next() + "\t" + key), NullWritable.get());
        }
    }

    // Job 5: Final count per feed
    public static class FinalCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length >= 1) {
                context.write(new Text(parts[0]), one);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Clean output directories
        if (fs.exists(new Path(OUTPUT_DIR + "/output_2"))) {
            fs.delete(new Path(OUTPUT_DIR + "/output_2"), true);
        }

        Path tempOutput1 = new Path(OUTPUT_DIR + "/output_2/temp1");
        Path tempOutput2 = new Path(OUTPUT_DIR + "/output_2/temp2");
        Path tempOutput3 = new Path(OUTPUT_DIR + "/output_2/temp3");
        Path tempOutput4 = new Path(OUTPUT_DIR + "/output_2/temp4");
        Path finalOutput = new Path(OUTPUT_DIR + "/output_2/final");

        if (fs.exists(tempOutput1)) fs.delete(tempOutput1, true);
        if (fs.exists(tempOutput2)) fs.delete(tempOutput2, true);
        if (fs.exists(tempOutput3)) fs.delete(tempOutput3, true);
        if (fs.exists(tempOutput4)) fs.delete(tempOutput4, true);
        if (fs.exists(finalOutput)) fs.delete(finalOutput, true);

        // Job 1: Process data and deduplicate within each feed
        Job job1 = Job.getInstance(conf, "Process and Dedup Within Feed");
        job1.setJarByClass(CaseStudy2DisconnectedStopsPerFeed.class);
        job1.setMapperClass(ProcessMapper.class);
        job1.setReducerClass(ProcessReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(DATASET_DIR + "/stops"));
        FileOutputFormat.setOutputPath(job1, tempOutput1);
        if (!job1.waitForCompletion(true)) System.exit(1);

        // Job 2: Round coordinates and filter to unique per feed
        Job job2 = Job.getInstance(conf, "Round and Filter Per Feed");
        job2.setJarByClass(CaseStudy2DisconnectedStopsPerFeed.class);
        job2.setMapperClass(RoundFilterMapper.class);
        job2.setReducerClass(RoundFilterReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, tempOutput1);
        FileOutputFormat.setOutputPath(job2, tempOutput2);
        if (!job2.waitForCompletion(true)) System.exit(1);

        // Job 3: Count unique rounded coordinates per feed (intermediate count)
        Job job3 = Job.getInstance(conf, "Count Per Feed");
        job3.setJarByClass(CaseStudy2DisconnectedStopsPerFeed.class);
        job3.setMapperClass(CountMapper.class);
        job3.setReducerClass(CountReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, tempOutput2);
        FileOutputFormat.setOutputPath(job3, tempOutput3);
        if (!job3.waitForCompletion(true)) System.exit(1);

        // Job 4: Filter out coordinates that appear in multiple feeds
        Job job4 = Job.getInstance(conf, "Cross-Feed Filter");
        job4.setJarByClass(CaseStudy2DisconnectedStopsPerFeed.class);
        job4.setMapperClass(CrossFeedFilterMapper.class);
        job4.setReducerClass(CrossFeedFilterReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job4, tempOutput2);
        FileOutputFormat.setOutputPath(job4, tempOutput4);
        if (!job4.waitForCompletion(true)) System.exit(1);

        // Job 5: Final count per feed
        Job job5 = Job.getInstance(conf, "Final Count");
        job5.setJarByClass(CaseStudy2DisconnectedStopsPerFeed.class);
        job5.setMapperClass(FinalCountMapper.class);
        job5.setReducerClass(CountReducer.class);
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(IntWritable.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job5, tempOutput4);
        FileOutputFormat.setOutputPath(job5, finalOutput);
        System.exit(job5.waitForCompletion(true) ? 0 : 1);
    }

}
