package cn.study001.FIndCSV;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import cn.stduy001.test.IntPair;

//排序类，mapper输出进行排序，按照省，市，区和评分进行排序

public class CSVSortComparator extends WritableComparator {
	protected CSVSortComparator(){
		super(InfoWritableComparable.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		InfoWritableComparable info1=(InfoWritableComparable)a;
		InfoWritableComparable info2=(InfoWritableComparable)b;
		if(!info1.getProvinceName().equals(info2.getProvinceName())){
			return info1.getProvinceName().compareTo(info2.getProvinceName());
		}else if(info1.getCityID()!=info2.getCityID()){
			return info1.getCityID()-info2.getCityID();
		}else if(!info1.getAreaName().equals(info2.getAreaName())){
			return info1.getAreaName().compareTo(info2.getAreaName());
		}else if(info1.getRating()!=info2.getRating()){
			return ((info1.getRating()-info2.getRating())>0? -1:1);
		}else if(info1.getProductRating()!=info2.getProductRating()){
			return ((info1.getProductRating()-info2.getProductRating())>0?-1:1);
		}else if(info1.getEnvironmentRating()!=info2.getEnvironmentRating()){
			return ((info1.getServiceRating()-info2.getServiceRating())>0?-1:1);
		}
		return 0;
	}
}
