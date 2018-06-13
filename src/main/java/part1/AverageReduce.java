package part1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public  class AverageReduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	private Logger logger = Logger.getLogger(AverageReduce.class);

	
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntWritable val : values) {
            sum += val.get();
            count++;
        }
        double  averageNum =  (double)sum/(double)count ;
        logger.info("("+key+", "+averageNum+")");
        context.write(key, new DoubleWritable(averageNum));
    }
 }