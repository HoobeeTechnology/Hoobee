package cn.study001.FIndCSV;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

 
public class CsvReaderMain {
	private static final String INPUT_PATH="hdfs://cluster1/FindCSVin";
	private static final String OUTPUT_PATH="hdfs://cluster1/FindCSVout";
	
	public static void main(String[] arg) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException{
		Configuration conf=new Configuration();
		String address = new String("first\t");  //初始化一个值，避免为空时，后面mapper 切词时报错
		for(String str:arg){
			address+=(str+"\t");		//传入main的参数，后面通过conf将值传至mapper 或reducer阶段使用
		}
		conf.set("address",address );
		Job job=new Job(conf, CsvReaderMain.class.getSimpleName());
		job.setJarByClass(CsvReaderMain.class);			//打成jar包的话 需要加这一行
		
		FileSystem fileSystem=FileSystem.get(new URI(INPUT_PATH), conf);
		if(fileSystem.exists(new Path(OUTPUT_PATH))){
			fileSystem.delete(new Path(OUTPUT_PATH), true);
		}
		FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ReadCSVMapper.class);
		job.setMapOutputKeyClass(InfoWritableComparable.class);
		job.setMapOutputValueClass(InfoWritableComparable.class);
		job.setPartitionerClass(HashPartitioner.class);
		job.setSortComparatorClass(CSVSortComparator.class);
		job.setGroupingComparatorClass(CSVGroupComparator.class);
		
		job.setReducerClass(CSVReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		job.waitForCompletion(true);
	}
}
