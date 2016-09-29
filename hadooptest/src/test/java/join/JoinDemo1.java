package join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinDemo1 {

	private static final Text typeA = new Text("A:");
	private static final Text typeB = new Text("B:");

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, MapWritable> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			super.map(key, value, context);
			/*
			  ±í1£º [plain] 
			  A:Beijing Red Star 1 
			  A:Shenzhen Thunder 3 
			  A:GuangzhouHonda 2 
			  A:Beijing Rising 1 
			  A:Guangzhou Development Bank 2
			  A:Tencent 3 
			  A:Back of Beijing 1
			  
			  ±í2£º [plain] 
			  B:1 Beijing 
			  B:2 Guangzhou 
			  B:3 Shenzhen 
			  B:4 Xian
			 */

			String valuestr = value.toString();
			String type = valuestr.substring(0, 2);
			String content = valuestr.substring(2);

			if (type.equals("A:")) {
				String[] contentArray = content.split("\t");
				String city = contentArray[0];
				String address = contentArray[1];
				MapWritable map = new MapWritable();
				map.put(typeA, new Text(city));
				context.write(new Text(address), map);

			}

			else if (type.equals("B:")) {
				String[] contentArray = content.split("\t");
				String adrNum = contentArray[0];
				String adrName = contentArray[1];
				MapWritable map = new MapWritable();
				map.put(typeB, new Text(adrName));
				context.write(new Text(adrNum), map);
			}
		}
	}
	
	public static class MyReduce extends Reducer<Text, MapWritable, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<MapWritable> values,
				Reducer<Text, MapWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator<MapWritable> it = values.iterator();
			ArrayList<Text> cityList = new ArrayList<Text>();
			ArrayList<Text> adrList = new ArrayList<Text>();
			
			while(it.hasNext()){
				
				MapWritable map = it.next();
				
				if(map.containsKey(typeA)){
					cityList.add((Text)map.get(typeA));
				}
				if(map.containsKey(typeB)){
					adrList.add((Text)map.get(typeB));  
				}	
			}
			
			for(int i = 0;i<=cityList.size();i++){
				for(int j = 0;j<=adrList.size();j++){
					context.write(cityList.get(i), adrList.get(j));
				}
			}
		}
	}
	
	

	public static void main(String[] args) {

	}

}
