package exp5;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
	矩阵相乘:
	1、转置评分矩阵
	2、相似度矩阵 与 (转置评分矩阵)相乘
	这里进行1：转置
	
	关于矩阵乘法为什么不采用传统的计算法则来计算？
	存在两个问题：
	1、不能并发执行，只能按照循环条件一次一次地执行
	2、如果矩阵的规模比较大，以至于放不到内存中，则可能要放入文件中，那么对于左侧的矩阵问题不大，每次只要读取一行放入内存中即可；
	   但是对于右侧矩阵来说，我们需要得到列向量，也就是要遍历所有的行，每行取一个元素，然后组成列向量，当文件很大时，速度太慢。
	解决问题：
	1、引入并发执行框架Hadoop，其中的map和reduce可以并发执行
	2、将右边矩阵转置，从而实现列向量转为行向量
	input:
	1	A_2,C_5
	2	A_10,B_3
	3	C_15
	4	A_3,C_5
	5	B_3
	6	A_5,B_5
	output:
	A	6_5,4_3,2_10,1_2
	B	6_5,5_3,2_3
	C	4_5,3_15,1_5
*/
public class Step3 {
	public static class Mapper3 extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)throws IOException, InterruptedException {
			String[] rowAndline = value.toString().split("\t");
			String row = rowAndline[0];
			String[] lines = rowAndline[1].split(",");
			for(String line : lines)
			{
				String colunm = line.split("_")[0];
				String valueStr = line.split("_")[1];
				outKey.set(colunm);
				//将列作为行
				outValue.set(row + "_" + valueStr);
				//将行作为列
				context.write(outKey, outValue);
			}
		}
		/*
			所有map操作产生
			 ("1","1_0")	("2","1_3") 	("3","1_-1")	("4","1_2")		("5","1_-3")
			（"1","2_1"）	("2","2_3") 	("3","2_5")	    ("4","2_-2")	("5","2_-1")
			（"1","3_0"）	("2","3_1")	    ("3","3_4")		("4","3_-1")	("5","3_2")
			（"1","4_-2"）  ("2","4_2")	    ("3","4_-1")	("4","4_1")		("5","4_2")
		*/
	}

	/*
		Reduce任务，将map操作产生的所有键值对集合进行合并，生成转置矩阵的存储表示
		key值相同的值会组成值的集合
		如：
		key:"1"时
		values:{"3_0","1_0","4_-2","2_1"} 
		注意：这里就是为什么要进行列标号的原因，values的顺序不一定就是原来矩阵列的顺序
	*/	
	public static class Reducer3 extends Reducer<Text,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			for(Text text : values)
			{
				sb.append(text + ",");
			}
			//sb : "3_0,1_0,4_-2,2_1,"
			//注意这里末尾有个逗号
			String line = "";
			if(sb.toString().endsWith(","))
			{
				line = sb.substring(0,sb.length()-1);
			}
			//去掉逗号
			//line : "3_0,1_0,4_-2,2_1"
			outKey.set(key);
			outValue.set(line);
			//("1","3_0,1_0,4_-2,2_1")
			context.write(outKey, outValue);
		}		
	}
	
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		String[] otherArgs = {args[0],args[1]};
		if (otherArgs.length != 2) {
		    System.err.println("Usage: wordcount <in> <out>");
		    System.exit(2);
		}
		    
		Job job = new Job(conf, "step3");
		job.setJarByClass(Step3.class);
		job.setMapperClass(Mapper3.class); //为job设置Mapper类 
		//job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类  
		job.setReducerClass(Reducer3.class); //为job设置Reduce类 

		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(Text.class); 

		job.setOutputKeyClass(Text.class);        //设置输出key的类型
		job.setOutputValueClass(Text.class);//  设置输出value的类型

		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径

		FileOutputFormat.setOutputPath(job, new Path(args[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(args[1]);
		if(fs.exists(outPath))
		{
			fs.delete(outPath, true);
		}
		    
		return job.waitForCompletion(true) ? 1 : -1;
	}
	
	public static void main(String[] args)
	{
		try {
			new Step3().run(args);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}	
}
