package cn.study001.FIndCSV;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//分组类，对mapper输出分组，相同的省为一组

public class CSVGroupComparator extends WritableComparator {
	public CSVGroupComparator(){
		super(InfoWritableComparable.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		InfoWritableComparable info1=(InfoWritableComparable)a;
		InfoWritableComparable info2=(InfoWritableComparable)b;
		return info1.getProvinceName().compareTo(info2.getProvinceName());
	}
}
