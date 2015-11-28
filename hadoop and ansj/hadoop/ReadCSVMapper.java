package cn.study001.FIndCSV;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ReadCSVMapper extends Mapper<LongWritable, Text, 
	InfoWritableComparable, InfoWritableComparable> {
	@Override
	protected void map(LongWritable key, Text value,Context context)
					throws IOException, InterruptedException {
		if(key.get()==0)
			return;
		ArrayList<String> list=new ArrayList<String>();
        Pattern p = Pattern.compile("\"(.*?)\"");	
        Matcher m = p.matcher(value.toString());
        while (m.find()) {
            list.add(m.group());
        }
        Configuration conf=context.getConfiguration();
        String arg = conf.get("address");
        String[] split = arg.split("\t");
        
        String ProvinceName=list.get(3).substring(1, list.get(3).length()-1);
        String CityName=list.get(4).substring(1, list.get(4).length()-1);
        String areaName=list.get(7).substring(1, list.get(7).length()-1);
        String addressName=list.get(12).substring(1, list.get(12).length()-1);
        String CityID=list.get(6).substring(1, list.get(6).length()-1);
        String ShopName=list.get(1).substring(1, list.get(1).length()-1);
        String productRating=list.get(31).substring(1, list.get(31).length()-1);
        String environmentRating=list.get(32).substring(1, list.get(32).length()-1);
        String serviceRating=list.get(33).substring(1, list.get(33).length()-1);
        
        if(productRating.length()==0) {
        	productRating="0";
        }
        if(environmentRating.length()==0) {
        	environmentRating="0";
        }
        if(serviceRating.length()==0) {
        	serviceRating="0";
        }
        InfoWritableComparable info=new InfoWritableComparable(CityName,Integer.parseInt(CityID),ShopName,Float.parseFloat(productRating),
        		Float.parseFloat(environmentRating),Float.parseFloat(serviceRating),ProvinceName,
        		areaName,addressName);
        switch (split.length) {
		case 1:
			context.write(info, info);
			break;
		case 2:
			if(split[1].equals(ProvinceName)){
				context.write(info, info);
			}
			break;
		case 3:
			if(split[1].equals(ProvinceName)&&split[2].equals(CityName)){
				context.write(info, info);
			}
			break;
		case 4:
			if(split[1].equals(ProvinceName)&&split[2].equals(CityName)&&split[3].equals(areaName)){
				context.write(info, info);
			}
			break;
		default:
			break;
		}
	}
}
