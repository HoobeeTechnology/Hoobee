package cn.study001.FIndCSV;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CSVReducer extends Reducer<InfoWritableComparable, InfoWritableComparable, 
	Text, NullWritable> {
	
	protected void reduce(InfoWritableComparable arg0, Iterable<InfoWritableComparable> arg1,Context arg2)
					throws IOException, InterruptedException {
		for(InfoWritableComparable info:arg1){
			String keyString=info.getProvinceName()+"\t"+info.getCityName()+"\t"+info.getAreaName()+"\t"
		+info.getShopName()+"\t"+info.getAddressName()+"\t"+info.getRating()+"\t"+info.getProductRating()+
					"\t"+info.getEnvironmentRating()+"\t"+info.getServiceRating();
			arg2.write(new Text(keyString), NullWritable.get());
			System.out.println(keyString);
		}
	}
}
