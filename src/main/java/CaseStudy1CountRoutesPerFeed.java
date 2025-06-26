import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CaseStudy1CountRoutesPerFeed {

    private static final String DATASET_DIR = "/Users/mmafrar/malaysia-gtfs-history";
    private static final String OUTPUT_DIR = "/Users/mmafrar/malaysia-gtfs-history";

    public static class RouteMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (isHeader && line.contains("feed_id")) {
                isHeader = false;
                return;
            }

            String[] fields = line.split(",", -1);
            if (fields.length < 6) return;

            String feedId = fields[0].trim();
            String routeLongName = fields[6].trim();

            if (!feedId.equals("KTMB") && !feedId.equals("Prasarana_RapidRailKL")) {
                context.write(new Text(feedId), new Text(routeLongName));
            }
        }
    }

    public static class DistinctRouteReducer extends Reducer<Text, Text, Text, IntWritable> {
        private Map<Text, Integer> routeCounts = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> distinctRoutes = new HashSet<>();
            for (Text val : values) {
                distinctRoutes.add(val.toString());
            }
            // Store the result for sorting later
            routeCounts.put(new Text(key), distinctRoutes.size());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort entries by value (size) descending
            List<Map.Entry<Text, Integer>> sorted = new ArrayList<>(routeCounts.entrySet());
            sorted.sort((a, b) -> b.getValue().compareTo(a.getValue()));
            for (Map.Entry<Text, Integer> entry : sorted) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Clean output directories
        if (fs.exists(new Path(OUTPUT_DIR + "/output_1"))) {
            fs.delete(new Path(OUTPUT_DIR + "/output_1"), true);
        }

        Job job = Job.getInstance(conf, "Distinct Route Count");
        job.setJarByClass(CaseStudy1CountRoutesPerFeed.class);

        job.setMapperClass(RouteMapper.class);
        job.setReducerClass(DistinctRouteReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(DATASET_DIR + "/routes"));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR + "/output_1/"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
