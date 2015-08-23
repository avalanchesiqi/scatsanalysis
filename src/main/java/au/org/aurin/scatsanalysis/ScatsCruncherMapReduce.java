package au.org.aurin.scatsanalysis;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ScatsCruncherMapReduce implements Tool{
  
  Configuration configuration;

  public static class ScatsCruncherMap extends Mapper<Object, Text, Text, IntWritable>{
    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String records = value.toString();
      StringTokenizer allRecords = new StringTokenizer(records, "\n");
      
      while(allRecords.hasMoreTokens()){
        String singleRecord = allRecords.nextToken();
        String[] tmp = parseRecord(singleRecord);
        for (String t: tmp) {
          Text siteAndDate = new Text(t.split("\t")[0]);
          int vechileCount = Integer.parseInt(t.split("\t")[1]);
          context.write(siteAndDate, new IntWritable(vechileCount));
        }
      }
    }
  }

  public static class ScatsCruncherReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
        throws IOException, InterruptedException {
      int totalVehicleCount = 0;
      Iterator<IntWritable> iterator = values.iterator();
      while(iterator.hasNext()){
        totalVehicleCount += iterator.next().get();
      }
      context.write(key, new IntWritable(totalVehicleCount));
    }
  }

  public int run(String [] args) throws Exception{
    
    Job job = new Job(getConf());
    job.setJarByClass(ScatsCruncherMapReduce.class);
    job.setJobName("ScatsCruncherMapReduce");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(ScatsCruncherMap.class);
    job.setCombinerClass(ScatsCruncherReduce.class);
    job.setReducerClass(ScatsCruncherReduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean success=  job.waitForCompletion(true);
    
    return success ? 0: 1;
    
  }
  
  
  public static void main(String[] args) throws Exception {
    
    int ret = ToolRunner.run(new ScatsCruncherMapReduce(), args);
    System.exit(ret);
  }
  
  @Override
  public Configuration getConf() {
    return configuration;
  }
  
  @Override
  public void setConf(Configuration conf) {
    conf = new Configuration();
    configuration=conf;
  }
  
  protected static String[] parseRecord(String line) {
    String[] records = new String[96];
    String[] parsedFields = line.replaceAll("\"", "").split(",");
    String siteNo = parsedFields[0];
    String date = parsedFields[1].split("\\s+")[0];
    int len = parsedFields.length;
    for (int i=0; i<24; i++) {
      for (int j=0; j<4; j++) {
        if (i*4+j+3 >= len) {
          records[i*4+j] = siteNo + "-" + date + "-" + Integer.toString(i)
              + "-" + Integer.toString(j+1) + " 0";
        } else {
          String vehicleCount = parsedFields[i*4+j+3];
          if (vehicleCount.equals("") || vehicleCount.charAt(0) == '-') {
            vehicleCount = "0";
          }
          records[i*4+j] = siteNo + "-" + date + "-" + Integer.toString(i)
              + "-" + Integer.toString(j+1) + " " + vehicleCount;
        }
      }
    }
    return records;
  }
}

