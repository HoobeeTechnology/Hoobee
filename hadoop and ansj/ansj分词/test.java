package cn.ansj.test;

 
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.nlpcn.commons.lang.util.IOUtil;


public class test {
	public static final String TAG_START_CONTENT="<content>";
	public static final String TAG_END_CONTENT="</content>";
	
	public static void main(String[] args) {
		 String temp=null;
		 BufferedReader reader=null;
		 PrintWriter pw=null;
		 try{
			 reader=IOUtil.getReader("news_tensite_xml.smarty.dat",IOUtil.GBK);
			 pw = new PrintWriter("resultbig.txt");  
			 long start=System.currentTimeMillis();
			 int allCount=0;
			 int termcnt=0;
			 Set<String> set=new HashSet<String>();
			 while((temp=reader.readLine())!=null){
				 temp=temp.trim();
				 if(temp.startsWith(TAG_START_CONTENT)){
					 int a=TAG_START_CONTENT.length();
					 String content=temp.substring(TAG_START_CONTENT.length()
							 , temp.indexOf(TAG_END_CONTENT));
					 
					 if(content.length()>0){
						 allCount+=content.length();
						 List<Term> result=ToAnalysis.parse(content);
						 for(Term term:result){
							 String item=term.getName().trim();
							 if(item.length()>0){
								 termcnt++;
								 pw.print(item.trim()+" ");
								 set.add(item);
							 }
						 }
						 pw.println();
					 }
				 }
			 }
			 long end=System.currentTimeMillis();
			 System.out.println("共"+termcnt+"个term，"+
			 set.size()+"个不同的词，共"+allCount+"个字符，每秒处理了："+
					 (allCount*1000)/(end-start));
		 }catch(IOException e){
			 e.printStackTrace();
		 }finally{
			 if(null!=reader){
				 try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			 }
			 if(null!=pw){
				 pw.close();
			 }
		 }
		 
	}
}

