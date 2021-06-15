package j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;


/**
 * Author: ysc
 * Time: 2021/5/21
 * Description:
 */
public class avg_score{

    public static void main(String[] args) throws IOException {
            SparkConf conf = new SparkConf();
            // 配置spark 本地
            conf.setMaster("local");
            conf.setAppName("WordCount");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaRDD<String> fileRdd = sc.textFile("hdfs://localhost:9000/input/grades.txt");
            // 通过text生成RDD
            JavaRDD<String> lineRDD = fileRdd.flatMap(s -> Arrays.asList(s.split("\n")).iterator());
            JavaPairRDD ScoreRDD = lineRDD.mapToPair(new PairFunction<String,Tuple3<String,String,String>,Integer>() {
                @Override
                public Tuple2<Tuple3<String,String,String>,Integer> call(String s) throws Exception {
                    String s1[] = s.split(",");
                    String sclass = s1[0];
                    String name = s1[1];
                    String bixiu= s1[3];
                    Integer score=Integer.parseInt(s1[4]);
                    return new Tuple2<Tuple3<String,String,String>,Integer>(new Tuple3<>(sclass,name,bixiu),score);
                }
            }).filter(line -> line._1()._3().equals("必修"))
                    .mapToPair((Tuple2<Tuple3<String,String,String>,Integer> input)->{
                        String sclass = input._1._1();
                        String name = input._1._2();
                        Integer score = input._2;
                        // 为了解决不同班级之间存在同名学生 这一问题，这里的主键设置为(班级，姓名)元组；
                        return new Tuple2<Tuple2<String,String>,Tuple2<Integer,Integer>>(new Tuple2<>(sclass,name),new Tuple2<>(score,1));
                    });
        //Function2 带两个参数的方法，声明需要三个泛型参数，前两个是输入参数类型，后一个是输出参数类型。
            JavaPairRDD<Tuple2<String,String>,Tuple2<Integer,Integer>> totalScoreRDD = ScoreRDD.reduceByKey(new Function2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>() {
                @Override
                public Tuple2<Integer,Integer> call(Tuple2<Integer,Integer> t1, Tuple2<Integer,Integer> t2) throws Exception {
                    Integer score2=t1._1()+t2._1();
                    Integer num=t1._2()+t2._2();
                    return new Tuple2<Integer,Integer>(score2,num);
                }
            });
//            System.out.println(totalScoreRDD.take(5));

            JavaPairRDD<Tuple2<String,String>,Double> avgScoreRDD = totalScoreRDD.mapToPair((Tuple2<Tuple2<String,String>,Tuple2<Integer,Integer>> input2)->{
                String sclass=input2._1()._1();
                String name=input2._1()._2();
                Integer totalScore=input2._2()._1();
                Integer num=input2._2()._2();
                DecimalFormat df = new DecimalFormat("0.00");//格式化小数
                String avg = df.format((float)totalScore/num);//返回的是String类型
                Double avgScore=Double.valueOf(avg);
                return new Tuple2<Tuple2<String,String>,Double>(new Tuple2<>(sclass,name),avgScore);
            });

            avgScoreRDD.saveAsTextFile("hdfs://localhost:9000/output3/result1");
           // JavaRDD<String> outRDD=avgScoreRDD.map(i->i.toString());
            //outRDD.saveAsTextFile("hdfs://localhost:9000/data/result");
//            List<String> stringList=outRDD.collect();
//           // BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath1));
//            for (String s:stringList){
//                  bw.write(s);
//                bw.newLine();
//            }
//            //bw.close();


            JavaPairRDD<String,Integer> numRDD=avgScoreRDD.mapToPair((Tuple2<Tuple2<String,String>,Double> input)->{
                Double score=input._2();
                String str;
                if(score.compareTo(90.0)>=0){
                    str="90-100";
                }else if(score.compareTo(80.0)>=0){
                    str="80-89";
                }else if(score.compareTo(70.0)>=0){
                    str="70-79";
                }else if(score.compareTo(60.0)>=0){
                    str="60-69";
                }else{
                    str="under 60";
                };
                return new Tuple2<String,Integer>(str,1);
            }).reduceByKey((x,y)->x+y);

            numRDD.saveAsTextFile("hdfs://localhost:9000/output3/result2");
            sc.stop();
    }
}
