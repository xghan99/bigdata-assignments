/*
ENTER YOUR NAME HERE
NAME: Han Xiao Guang
MATRICULATION NUMBER: [REDACTED]
*/
import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopkCommonWords {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable two = new IntWritable(2);
        private Text word = new Text();
        public HashSet<String> stopwords_set = new HashSet<>();
        public String path1;
        public String path2;

        public void setup (Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopwords_path = conf.get("stopwords");
            path1 = conf.get("p1");
            path1 = path1.substring(path1.lastIndexOf("/")+1);
            path2 = conf.get("p2");
            path2 = path2.substring(path2.lastIndexOf("/")+1);

            BufferedReader reader = new BufferedReader(new FileReader(stopwords_path));
            String line;
            while ((line=reader.readLine())!=null){
                stopwords_set.add(line);
            }

        }


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString()," \t\n\r\f");
            String input_path = context.getInputSplit().toString();
            while (itr.hasMoreTokens()) {
                String x = itr.nextToken();
                if (stopwords_set.contains(x) || x.length()<=4) {
                    continue;
                }
                word.set(x);
                if (input_path.contains(path1)) {
                    context.write(word, one);
                }
                if (input_path.contains(path2)) {
                    context.write(word, two);
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,IntWritable,Text> {

        Comparator<String> treeSetComparator = new Comparator<>(){
            public int compare(String k1, String k2){
                int v1 = result_map.get(k1);
                int v2 = result_map.get(k2);
                if (v1>v2) {
                    return 1;
                }
                else if (v2>v1) {
                    return -1;
                }
                else if (k1.compareTo(k2)<0){
                    return 1;
                }
                else {
                    return -1;
                }


            }
        };
        public HashMap<String,Integer> result_map = new HashMap<>();
        public TreeSet<String> sorted_keys = new TreeSet<>(treeSetComparator);
        private Text key = new Text();
        private IntWritable result = new IntWritable();
        public void cleanup (Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = conf.getInt("k",10);
            for (int i=0;i<k;i++){
                String x = sorted_keys.pollLast();
                key.set(x);
                result.set(result_map.get(x));
                context.write(result, key);
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum1 = 0;
            int sum2 = 0;
            for (IntWritable val : values) {

                int x = val.get();
                if (x==1) {
                    sum1++;
                }
                if (x==2) {
                    sum2++;
                }
            }
            int y = Math.min(sum1,sum2);


            String z = key.toString();

            result_map.put(z, y);
            sorted_keys.add(z);


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[4]);
        conf.set("stopwords", args[2]);
        conf.set("p1",args[0]);
        conf.set("p2", args[1]);
        conf.setInt("k",k);
        Job job = Job.getInstance(conf, "topk");
        job.setJarByClass(TopkCommonWords.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

