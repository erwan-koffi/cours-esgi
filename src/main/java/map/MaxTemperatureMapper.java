package map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;
    private static final char PLUS = '+';
    private static final String QUALITY_MATCHER = "[01459]";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        String parsableTemperature;
        if (line.charAt(87) == PLUS) {
            parsableTemperature = line.substring(88, 92);
        } else {
            parsableTemperature = line.substring(87, 92);
        }
        String quality = line.substring(92, 93);
        int airTemperature = Integer.parseInt(parsableTemperature);
        if (airTemperature != MISSING && quality.matches(QUALITY_MATCHER)) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
