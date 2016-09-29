package deptemp;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class Q3DeptEarliestEmp extends Configured implements Tool {

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
                Path[] paths =     DistributedCache.getLocalCacheFiles(context.getConfiguration());
                String deptIdName = null;
                for (Path path : paths) {
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

        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

            // ��Ա���ļ��ֶν��в��
            kv = value.toString().split(",");

            // map join: ��map�׶ι��˵�����Ҫ������
            // ���keyΪ�������ƺ�valueΪԱ������+","+Ա�����빫˾����
            if (deptMap.containsKey(kv[7])) {
                if (null != kv[4] && !"".equals(kv[4].toString())) {
                    context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[1].trim()                     + "," + kv[4].trim()));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,         InterruptedException {

            // Ա�������ͽ��빫˾����
            String empName = null;
            String empEnterDate = null;

            // ��������ת����ʽ��������빫˾��Ա��������
            DateFormat df = new SimpleDateFormat("dd-MM��-yy");

            Date earliestDate = new Date();
            String earliestEmp = null;

            // �����ò���������Ա�����õ�������빫˾��Ա����Ϣ
            for (Text val : values) {
                empName = val.toString().split(",")[0];
                empEnterDate = val.toString().split(",")[1].toString().trim();
                try {
                    System.out.println(df.parse(empEnterDate));
                    if (df.parse(empEnterDate).compareTo(earliestDate) < 0) {
                        earliestDate = df.parse(empEnterDate);
                        earliestEmp = empName;
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            // ���keyΪ�������ƺ�valueΪ�ò���������빫˾Ա��
            context.write(key, new Text("The earliest emp of dept:" + earliestEmp + ", Enter             date:" + new SimpleDateFormat("yyyy-MM-dd").format(earliestDate)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // ʵ������ҵ����������ҵ����
        Job job = new Job(getConf(), "Q3DeptEarliestEmp");
        job.setJobName("Q3DeptEarliestEmp");

        // ����Mapper��Reduce��
        job.setJarByClass(Q3DeptEarliestEmp.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        // ���������ʽ��
        job.setInputFormatClass(TextInputFormat.class);

        // ���������ʽ��
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ��1������Ϊ����Ĳ�������·������2������ΪԱ������·���͵���������Ϊ���·��
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
        int res = ToolRunner.run(new Configuration(), new Q3DeptEarliestEmp(), args);
        System.exit(res);
    }
}