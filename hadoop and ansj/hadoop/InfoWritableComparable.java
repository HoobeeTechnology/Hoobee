package cn.study001.FIndCSV;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//自定义接口类型 实现WritableComparable，用此来当mapper的key，
//必须实现其序列化读写

public class InfoWritableComparable implements WritableComparable<InfoWritableComparable> {
	private String cityName;
	private int cityID;
	private String shopName;
	private float rating;
	private float productRating;
	private float environmentRating;
	private float serviceRating;
	private String provinceName;
	private String areaName;
	private String addressName;
	
	public InfoWritableComparable(){}
	public InfoWritableComparable(String cityName,int cityID,String shopName,float productRating, float environmentRating,
			float serviceRating,String ProvinceName,String areaName,String addressName){
		this.cityName=cityName;
		this.cityID=cityID;
		this.shopName=shopName;
		this.productRating=productRating;
		this.environmentRating=environmentRating;
		this.serviceRating=serviceRating;
		this.rating=(productRating+environmentRating+serviceRating)/3;
		this.provinceName=ProvinceName;
		this.areaName=areaName;
		this.addressName=addressName;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(cityName);
		out.writeInt(cityID);
		out.writeUTF(shopName);
		out.writeFloat(rating);
		out.writeFloat(productRating);
		out.writeFloat(environmentRating);
		out.writeFloat(serviceRating);
		out.writeUTF(provinceName);
		out.writeUTF(areaName);
		out.writeUTF(addressName);
	}

	public String getCityName() {
		return cityName;
	}
	public void setCityName(String cityName) {
		this.cityName = cityName;
	}
	public int getCityID() {
		return cityID;
	}
	public void setCityID(int cityID) {
		this.cityID = cityID;
	}
	public String getShopName() {
		return shopName;
	}
	public void setShopName(String shopName) {
		this.shopName = shopName;
	}
	public float getRating() {
		return rating;
	}
	public void setRating(float rating) {
		this.rating = rating;
	}
	public float getProductRating() {
		return productRating;
	}
	public void setProductRating(float productRating) {
		this.productRating = productRating;
	}
	public float getEnvironmentRating() {
		return environmentRating;
	}
	public void setEnvironmentRating(float environmentRating) {
		this.environmentRating = environmentRating;
	}
	public float getServiceRating() {
		return serviceRating;
	}
	public void setServiceRating(float serviceRating) {
		this.serviceRating = serviceRating;
	}
	public void readFields(DataInput in) throws IOException {
		this.cityName=in.readUTF();
		this.cityID=in.readInt();
		this.shopName=in.readUTF();
		this.rating=in.readFloat();
		this.productRating=in.readFloat();
		this.environmentRating=in.readFloat();
		this.serviceRating=in.readFloat();
		this.provinceName=in.readUTF();
		this.areaName=in.readUTF();
		this.addressName=in.readUTF();
	}

	public String getProvinceName() {
		return provinceName;
	}
	public void setProvinceName(String provinceName) {
		this.provinceName = provinceName;
	}
	public String getAreaName() {
		return areaName;
	}
	public void setAreaName(String areaName) {
		this.areaName = areaName;
	}
	public String getAddressName() {
		return addressName;
	}
	public void setAddressName(String addressName) {
		this.addressName = addressName;
	}
	public int compareTo(InfoWritableComparable o) {
		return 0;
	}
}
