import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Wordcount{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Wordcount")
    val sc = new SparkContext(conf)
    val book = sc.textFile("shakespeare.txt") 
    val words = book.flatMap(_.split("\\s"))
                    .map(_.replaceAll("[,.!?:;]","")
                    .trim
                    .toLowerCase)
                    .filter(!_.isEmpty)
                    .map((_,1))
    val counts = words.reduceByKey(_+_).sortBy(_._2,false)
    counts.take(10).map(println)
  } 
}
