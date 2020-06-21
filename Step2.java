package exp5;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
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
	计算每两行的相似度，最终形成一个相似度矩阵 
	input
	movieID [userID_sumScore...]
	1	A_2,C_5
	2	A_10,B_3
	3	C_15
	4	A_3,C_5
	5	B_3
	6	A_5,B_5
	同时将input拷贝一份到缓存cache中，然后对input和cache的每一行就行求值
	output:
	movieID	[itemID_cos...]
	1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
	2	1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
	3	4_0.86,3_1.00,1_0.93
	4	1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
	5	2_0.29,5_1.00,6_0.71
	6	1_0.26,5_0.71,6_1.00,2_0.88,4_0.36
 */
public class Step2 {	
	/*
		input:
		movieID [userID_sumScore...]
		1	A_2,C_5
		2	A_10,B_3
		3	C_15
		4	A_3,C_5
		5	B_3
		6	A_5,B_5
		cache : = input
		output:
		1	1_1.00
		1	2_0.36
		1	3_0.93
		1	4_0.99
		1	6_0.26
		2	1_0.36
		2	2_1.00
		2	4_0.49
		2	5_0.29
		2	6_0.88
		3	1_0.93
		3	3_1.00
		3	4_0.86
		4	1_0.99
		4	2_0.49
		4	3_0.86
		4	4_1.00
		4	6_0.36
		5	2_0.29
		5	5_1.00
		5	6_0.71
		6	1_0.26
		6	2_0.88
		6	4_0.36
		6	5_0.71
		6	6_1.00
	*/
	public static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		private List<String> cacheList = new ArrayList<String>();
		
		private DecimalFormat df = new DecimalFormat("0.00");
		
		/*
			将文件缓存到内存中，每一行为一个字符串，是所有行构成list
		*/
		protected void setup(Context context)throws IOException, InterruptedException {
			FileReader fr = new FileReader("itemUserScore1");
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while((line = br.readLine()) != null)
			{
				cacheList.add(line);
				//System.out.println("123456789");
				System.out.println(line);
			}
			fr.close();
			br.close();
		}
		
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			System.out.println("map,key=" + key + ",value=" + value.toString());
			String[] rowAndline = value.toString().split("\t");
			//获得行号
			String row_matrix1 = rowAndline[0];
			String[] column_value_array_matrix1 = rowAndline[1].split(",");
			//获得各列
			
			//|x|=sqrt(x1^2+x2^2+...)
			double denominator1 = 0;
			//定义向量1的模
			for(String colunm : column_value_array_matrix1)//对于向量1的每一个分量
			{
				String score = colunm.split("_")[1];
				denominator1 +=  Double.valueOf(score) * Double.valueOf(score);
				//计算分量的平方并累加到模
			}
			denominator1 = Math.sqrt(denominator1);//开跟号得到模
			
			
			for(String line : cacheList)// 以line 2	A_10,B_3  为例
			{
				String[] rowAndline2 = line.toString().split("\t");
				String row_matrix2 = rowAndline2[0];
				String[] column_value_array_matrix2 = rowAndline2[1].split(",");
				
				double denominator2 = 0;//求向量2的模
				for(String colunm : column_value_array_matrix2)
				{
					String score = colunm.split("_")[1];
					denominator2 +=  Double.valueOf(score) * Double.valueOf(score);
				}
				denominator2 = Math.sqrt(denominator2);
				
				
				int numerator = 0;
				//保存成绩累加结果
				for(String column_value_matrix1 : column_value_array_matrix1)
				{
					String column_maxtrix1 = column_value_matrix1.split("_")[0];
					//获得用户ID
					String value_matrix1 = column_value_matrix1.split("_")[1];
					//获得分数
					
					for(String column_value_matrix2 : column_value_array_matrix2)//对于向量2的每一列(分量) A_10,B_3
					{
						String column_maxtrix2 = column_value_matrix2.split("_")[0];
						//获得用户ID
						String value_matrix2 = column_value_matrix2.split("_")[1];
						//获得分数
						
						//如果是同一个分量
						if(column_maxtrix2.equals(column_maxtrix1))//这里也体现了为什么要标明列号，只有列号明确且相等，才证明是同一个位置的分量
						{
							numerator += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
							//numerator += 2×10
						}
					}
				}
				
				double cos = numerator / (denominator1 * denominator2);
				//求余弦
				if(cos == 0)
					continue;
				outKey.set(row_matrix1);//输出的key值设置为左侧矩阵的行号
				outValue.set(row_matrix2 + "_" + df.format(cos));//输出的value值设置为右侧转置矩阵的行号(实际矩阵的列号)_该位置的值
				context.write(outKey, outValue);
				System.out.println(outKey + "\t" + outValue);
			}
		}
	}
	
	/*
		input:
		("1",["1_1.00","2_0.36","3_0.93","4_0.99","6_0.26"])
		("2",["1_0.36","2_1.00","4_0.49","5_0.29","6_0.88"])
		("3",["4_0.86","3_1.00","1_0.93"])
		("4",["1_0.99","4_1.00","6_0.36","3_0.86","2_0.49"])
		("5",["2_0.29","5_1.00","6_0.71"])
		("6",["1_0.26","5_0.71","6_1.00","2_0.88","4_0.36"])
		
		output:
		1	1_1.00,2_0.36,3_0.93,4_0.99,6_0.26
		2	1_0.36,2_1.00,4_0.49,5_0.29,6_0.88
		3	4_0.86,3_1.00,1_0.93
		4	1_0.99,4_1.00,6_0.36,3_0.86,2_0.49
		5	2_0.29,5_1.00,6_0.71
		6	1_0.26,5_0.71,6_1.00,2_0.88,4_0.36

		即将分量连起来
		得到最终的相似度矩阵
	*/
	public static class Reducer2 extends Reducer<Text,Text,Text,Text>
	{
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for(Text text : values)
			{
				sb.append(text + ",");
			}
			String line = "";
			if(sb.toString().endsWith(","))
			{
				line = sb.substring(0,sb.length()-1);
			}
			outKey.set(key);
			outValue.set(line);
			System.out.println(key+" "+line);
			context.write(outKey, outValue);
		}
		
	}
	
	
	
	private static final String CACHE = "input/cachestep2.txt";
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		String[] otherArgs = {args[0],args[1]};
		    
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "step2");//Job(Configuration conf, String jobName) 设置job名称和
		job.setJarByClass(Step2.class);
		job.setMapperClass(Mapper2.class); //为job设置Mapper类 
		//job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类  
		job.setReducerClass(Reducer2.class); //为job设置Reduce类 

		job.addCacheArchive(new URI(CACHE + "#itemUserScore1"));
		    
		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(Text.class); 

		job.setOutputKeyClass(Text.class);        //设置输出key的类型
		job.setOutputValueClass(Text.class);//  设置输出value的类型

		//TODO  
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(args[1]);
		if(fs.exists(outPath))
		{
			//fs.delete(outPath, true);
		}
		return job.waitForCompletion(true) ? 1 : -1;		
	}
	
	public static void main(String[] args)
	{
		try {
			new Step2().run(args);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}
