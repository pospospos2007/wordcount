import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;


	
	 public  class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		 
		 	private Logger logger = Logger.getLogger(Map.class);
		 
		    private final static IntWritable one = new IntWritable(1);
		    
		    private Text word = new Text();
		        
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		        String line = value.toString();
		        StringTokenizer tokenizer = new StringTokenizer(line);
		        String num = "";
		        while (tokenizer.hasMoreTokens()) {
		        	num = tokenizer.nextToken();
		        }
		        if(!num.equals("-")){
		        	word.set(num);
		        	System.out.println("system.out:("+key+","+num+")");
		            logger.info("log4j: ("+key+","+num+")");
		            context.write(word, one);
		        }
		    }
		 } 
