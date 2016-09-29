package deptemp;
/***
 * 3��Ӧ�ð���3.1 ��������1����������ŵ��ܹ���3.1.1 �������
MapReduce�е�join��Ϊ�ü��֣������������ reduce side join��map side join��semi join �ȡ�
reduce join ��shuffle�׶�Ҫ���д��������ݴ��䣬����ɴ���������IOЧ�ʵ��£���map side join �ڴ�����С��������ʱ�ǳ����� ��
Map side join��������³������е��Ż������������ӱ��У���һ����ǳ��󣬶���һ����ǳ�С��������С�����ֱ�Ӵ�ŵ��ڴ��С�
�������ǿ��Խ�С���ƶ�ݣ���ÿ��map task�ڴ��д���һ�ݣ������ŵ�hash table�У���
Ȼ��ֻɨ�������ڴ���е�ÿһ����¼key/value����hash table�в����Ƿ�����ͬ��key�ļ�¼������У������Ӻ�������ɡ�
Ϊ��֧���ļ��ĸ��ƣ�Hadoop�ṩ��һ����DistributedCache��ʹ�ø���ķ������£�
��1���û�ʹ�þ�̬����DistributedCache.addCacheFile()ָ��Ҫ���Ƶ��ļ���
���Ĳ������ļ���URI�������HDFS�ϵ��ļ�������������hdfs://jobtracker:50030/home/XXX/file����
JobTracker����ҵ����֮ǰ���ȡ���URI�б�������Ӧ���ļ�����������TaskTracker�ı��ش����ϡ�
��2���û�ʹ��DistributedCache.getLocalCacheFiles()������ȡ�ļ�Ŀ¼����ʹ�ñ�׼���ļ���дAPI��ȡ��Ӧ���ļ���
����������У������������С�ı�(����dept���������ڴ��У���Mapper�׶ζ�Ա�����ű��ӳ��ɲ������ƣ�
��������Ϊkey�����Reduce�У���Reduce�м��㰴�ղ��ż���������ŵ��ܹ��ʡ�


EMP:
7369,SMITH,CLERK,7902,17-12��-80,800,,20
7499,ALLEN,SALESMAN,7698,20-2��-81,1600,300,30
7521,WARD,SALESMAN,7698,22-2��-81,1250,500,30
7566,JONES,MANAGER,7839,02-4��-81,2975,,20
7654,MARTIN,SALESMAN,7698,28-9��-81,1250,1400,30
7698,BLAKE,MANAGER,7839,01-5��-81,2850,,30
7782,CLARK,MANAGER,7839,09-6��-81,2450,,10
7839,KING,PRESIDENT,,17-11��-81,5000,,10
7844,TURNER,SALESMAN,7698,08-9��-81,1500,0,30
7900,JAMES,CLERK,7698,03-12��-81,950,,30
7902,FORD,ANALYST,7566,03-12��-81,3000,,20
7934,MILLER,CLERK,7782,23-1��-82,1300,,10

DEPT:
10,ACCOUNTING,NEW YORK
20,RESEARCH,DALLAS
30,SALES,CHICAGO
40,OPERATIONS,BOSTON

 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Q1SumDeptSalary extends Configured implements Tool {

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        // ���ڻ��� dept�ļ��е�����
        private Map<String, String> deptMap = new HashMap<String, String>();
        private String[] kv;

        // �˷�������Map����ִ��֮ǰִ����ִ��һ��
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader in = null;
            try {

                // �ӵ�ǰ��ҵ�л�ȡҪ������ļ�
                Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {

                    // �Բ����ļ��ֶν��в�ֲ����浽deptMap��
                    if (path.toString().contains("dept")) {
                        in = new BufferedReader(new FileReader(path.toString()));
                        while (null != (deptIdName = in.readLine())) {
                            
                            // �Բ����ļ��ֶν��в�ֲ����浽deptMap��
                            // ����Map��keyΪ���ű�ţ�valueΪ���ڲ�������
                            deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // ��Ա���ļ��ֶν��в��
            kv = value.toString().split(",");

            // map join: ��map�׶ι��˵�����Ҫ�����ݣ����keyΪ�������ƺ�valueΪԱ������
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[5] && !"".equals(kv[5].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {

public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // ��ͬһ���ŵ�Ա�����ʽ������
            long sumSalary = 0;
            for (Text val : values) {
                sumSalary += Long.parseLong(val.toString());
            }

            // ���keyΪ�������ƺ�valueΪ�ò���Ա�������ܺ�
            context.write(key, new LongWritable(sumSalary));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // ʵ������ҵ����������ҵ���ơ�Mapper��Reduce��
        Job job = new Job(getConf(), "Q1SumDeptSalary");
        job.setJobName("Q1SumDeptSalary");
        job.setJarByClass(Q1SumDeptSalary.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // ���������ʽ��
        job.setInputFormatClass(TextInputFormat.class);

        // ���������ʽ
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ��1������Ϊ����Ĳ�������·������2������ΪԱ������·���͵�3������Ϊ���·��
    String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), args).getRemainingArgs();
    DistributedCache.addCacheFile(new Path(otherArgs[0]).toUri(), job.getConfiguration());
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        job.waitForCompletion(true);
        return job.isSuccessful() ? 0 : 1;
    }

    /**
     * ��������ִ�����
     * @param args �������
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Q1SumDeptSalary(), args);
        System.exit(res);
    }
}