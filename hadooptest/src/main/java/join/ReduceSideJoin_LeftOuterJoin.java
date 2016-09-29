package join;   
import java.io.IOException;   
import java.util.ArrayList;   

import org.apache.hadoop.conf.Configuration;   
import org.apache.hadoop.conf.Configured;   
import org.apache.hadoop.fs.Path;   
import org.apache.hadoop.io.Text;   
import org.apache.hadoop.mapreduce.Job;   
import org.apache.hadoop.mapreduce.Mapper;   
import org.apache.hadoop.mapreduce.Reducer;   
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;   
import org.apache.hadoop.mapreduce.lib.input.FileSplit;   
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;   
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;   
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;   
import org.apache.hadoop.util.Tool;   
import org.apache.hadoop.util.ToolRunner;  

import java.io.DataInput;   
import java.io.DataOutput;   

import org.apache.hadoop.io.WritableComparable;   
import org.apache.log4j.Logger;
/**   
 * @author zengzhaozheng   
 * ��;˵����   
 * reudce side join�е�left outer join   
 * �����ӣ������ļ��ֱ����2����,�����ֶ�table1��id�ֶκ�table2��cityID�ֶ�   
 * table1(���):tb_dim_city(id int,name string,orderid int,city_code,is_show)   
 * tb_dim_city.dat�ļ�����,�ָ���Ϊ"|"��   
 * id     name  orderid  city_code  is_show   
 * 0       ����        9999     9999         0   
 * 1       ����        1        901          1   
 * 2       ����        2        902          1   
 * 3       ��ƽ        3        903          1   
 * 4       ��ԭ        4        904          1   
 * 5       ͨ��        5        905          1   
 * 6       ��Դ        6        906          1   
 * 7       �׳�        7        907          1   
 * 8       ��ɽ        8        908          1   
 * 9       �Ӽ�        9        909          1   
 * -------------------------��ɧ�ķָ���-------------------------------   
 * table2(�ұ�)��tb_user_profiles(userID int,userName string,network string,double flow,cityID int)   
 * tb_user_profiles.dat�ļ�����,�ָ���Ϊ"|"��   
 * userID   network     flow    cityID   
 * 1           2G       123      1   
 * 2           3G       333      2   
 * 3           3G       555      1   
 * 4           2G       777      3   
 * 5           3G       666      4   
 *   
 * -------------------------��ɧ�ķָ���-------------------------------   
 *  �����   
 *  1   ����  1   901 1   1   2G  123   
 *  1   ����  1   901 1   3   3G  555   
 *  2   ����  2   902 1   2   3G  333   
 *  3   ��ƽ  3   903 1   4   2G  777   
 *  4   ��ԭ  4   904 1   5   3G  666   
 */ 

public class ReduceSideJoin_LeftOuterJoin extends Configured implements Tool{   
	public static class CombineValues implements WritableComparable<CombineValues>{  
		
	   // private static final Logger logger = LoggerFactory.getLogger(CombineValues.class);   
	    private Text joinKey;//���ӹؼ���   
	    private Text flag;//�ļ���Դ��־   
	    private Text secondPart;//�������Ӽ������������   
	    public void setJoinKey(Text joinKey) {   
	        this.joinKey = joinKey;   
	    }   
	    public void setFlag(Text flag) {   
	        this.flag = flag;   
	    }   
	    public void setSecondPart(Text secondPart) {   
	        this.secondPart = secondPart;   
	    }   
	    public Text getFlag() {   
	        return flag;   
	    }   
	    public Text getSecondPart() {   
	        return secondPart;   
	    }   
	    public Text getJoinKey() {   
	        return joinKey;   
	    }   
	    public CombineValues() {   
	        this.joinKey =  new Text();   
	        this.flag = new Text();   
	        this.secondPart = new Text();   
	    }
	 
	    @Override 
	    public void write(DataOutput out) throws IOException {   
	        this.joinKey.write(out);   
	        this.flag.write(out);   
	        this.secondPart.write(out);   
	    }   
	    @Override 
	    public void readFields(DataInput in) throws IOException {   
	        this.joinKey.readFields(in);   
	        this.flag.readFields(in);   
	        this.secondPart.readFields(in);   
	    }   
	    @Override 
	    public int compareTo(CombineValues o) {   
	        return this.joinKey.compareTo(o.getJoinKey());   
	    }   
	    @Override 
	    public String toString() {   
	        // TODO Auto-generated method stub   
	        return "[flag="+this.flag.toString()+",joinKey="+this.joinKey.toString()+",secondPart="+this.secondPart.toString()+"]";   
	    }   
	} 
	
	
	
	
	
	
	
   // private static final Logger logger = LoggerFactory.getLogger(ReduceSideJoin_LeftOuterJoin.class);   
    public static class LeftOutJoinMapper extends Mapper<Object, Text, Text, CombineValues> {   
        private CombineValues combineValues = new CombineValues();   
        private Text flag = new Text();   
        private Text joinKey = new Text();   
        private Text secondPart = new Text();   
        @Override 
        protected void map(Object key, Text value, Context context)   
                throws IOException, InterruptedException {   
            //����ļ�����·��   
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();   
            //��������tb_dim_city.dat�ļ�,��־��Ϊ"0"   
            if(pathName.endsWith("tb_dim_city.dat")){   
                String[] valueItems = value.toString().split("\\|");   
                //���˸�ʽ����ļ�¼   
                if(valueItems.length != 5){   
                    return;   
                }   
                flag.set("0");   
                joinKey.set(valueItems[0]);   
                secondPart.set(valueItems[1]+"\t"+valueItems[2]+"\t"+valueItems[3]+"\t"+valueItems[4]);   
                combineValues.setFlag(flag);   
                combineValues.setJoinKey(joinKey);   
                combineValues.setSecondPart(secondPart);   
                context.write(combineValues.getJoinKey(), combineValues);
 
                }//����������tb_user_profiles.dat����־��Ϊ"1"   
            else if(pathName.endsWith("tb_user_profiles.dat")){   
                String[] valueItems = value.toString().split("\\|");   
                //���˸�ʽ����ļ�¼   
                if(valueItems.length != 4){   
                    return;   
                }   
                flag.set("1");   
                joinKey.set(valueItems[3]);   
                secondPart.set(valueItems[0]+"\t"+valueItems[1]+"\t"+valueItems[2]);   
                combineValues.setFlag(flag);   
                combineValues.setJoinKey(joinKey);   
                combineValues.setSecondPart(secondPart);   
                context.write(combineValues.getJoinKey(), combineValues);   
            }   
        }   
    }   
    public static class LeftOutJoinReducer extends Reducer<Text, CombineValues, Text, Text> {   
        //�洢һ�������е������Ϣ   
        private ArrayList<Text> leftTable = new ArrayList<Text>();   
        //�洢һ�������е��ұ���Ϣ   
        private ArrayList<Text> rightTable = new ArrayList<Text>();   
        private Text secondPar = null;   
        private Text output = new Text();   
        /**   
         * һ���������һ��reduce����   
         */ 
        @Override 
        protected void reduce(Text key, Iterable<CombineValues> value, Context context)   
                throws IOException, InterruptedException {   
            leftTable.clear();   
            rightTable.clear();   
            /**   
             * �������е�Ԫ�ذ����ļ��ֱ���д��   
             * ���ַ���Ҫע������⣺   
             * ���һ�������ڵ�Ԫ��̫��Ļ������ܻᵼ����reduce�׶γ���OOM��   
             * �ڴ���ֲ�ʽ����֮ǰ������˽����ݵķֲ���������ݲ�ͬ�ķֲ���ȡ��   
             * �ʵ��Ĵ�����������������Ч�ķ�ֹ����OOM�����ݹ�����б���⡣   
             */ 
            for(CombineValues cv : value){   
                secondPar = new Text(cv.getSecondPart().toString());   
                //���tb_dim_city   
                if("0".equals(cv.getFlag().toString().trim())){   
                    leftTable.add(secondPar);   
                }   
                //�ұ�tb_user_profiles   
                else if("1".equals(cv.getFlag().toString().trim())){   
                    rightTable.add(secondPar);   
                }   
            }   
           
            for(Text leftPart : leftTable){   
                for(Text rightPart : rightTable){   
                    output.set(leftPart+ "\t" + rightPart);   
                    context.write(key, output);   
                }   
            }   
        }   
    }   
    @Override 
    public int run(String[] args) throws Exception {   
          Configuration conf=getConf(); //��������ļ�����   
            Job job=new Job(conf,"LeftOutJoinMR");   
            job.setJarByClass(ReduceSideJoin_LeftOuterJoin.class);
            FileInputFormat.addInputPath(job, new Path(args[0])); //����map�����ļ�·��   
            FileOutputFormat.setOutputPath(job, new Path(args[1])); //����reduce����ļ�·��
            job.setMapperClass(LeftOutJoinMapper.class);   
            job.setReducerClass(LeftOutJoinReducer.class);
            job.setInputFormatClass(TextInputFormat.class); //�����ļ������ʽ   
            job.setOutputFormatClass(TextOutputFormat.class);//ʹ��Ĭ�ϵ�output���ʽ
 
            //����map�����key��value����   
            job.setMapOutputKeyClass(Text.class);   
            job.setMapOutputValueClass(CombineValues.class);
 
            //����reduce�����key��value����   
            job.setOutputKeyClass(Text.class);   
            job.setOutputValueClass(Text.class);   
            job.waitForCompletion(true);   
            return job.isSuccessful()?0:1;   
    }   
    public static void main(String[] args) throws IOException,   
            ClassNotFoundException, InterruptedException {   
        try {   
            int returnCode =  ToolRunner.run(new ReduceSideJoin_LeftOuterJoin(),args);   
            System.exit(returnCode);   
        } catch (Exception e) {   
            // TODO Auto-generated catch block   
        }   
    }   
} 