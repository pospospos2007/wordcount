package part1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

	 public  class AverageMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		 
		 	private Logger logger = Logger.getLogger(AverageMap.class);
		 
		    private final static IntWritable one = new IntWritable(1);
		    
		    private Text word = new Text();
		        
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String line = value.toString();
		        StringTokenizer tokenizer = new StringTokenizer(line);
		        String num = "";
				String ip = "";
				boolean isFirst = true;
		        while (tokenizer.hasMoreTokens()) {
		        	num = tokenizer.nextToken();
					if(isFirst){
						ip = num;
						isFirst = false;
					}
		        }
				if(!num.equals("-")){
					word.set(ip);
					logger.info("("+ip+","+num+")");
					context.write(word, new IntWritable(Integer.valueOf(num)));
				}
}
		}
