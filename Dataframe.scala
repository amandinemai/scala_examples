import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Dataframe{
  def main(args: Array[String]){
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val schema = StructType(Array(
        StructField("email",StringType,true),
        StructField("id",IntegerType,true),
        StructField("location",StructType(Array(
        StructField("address",StringType,true),
        StructField("city",StringType,true),
        StructField("state",StringType,true),
        StructField("zipcode",StringType,true)))),
        StructField("name", StringType, true),
        StructField("phone", StringType, true)
    ))

    val clients = spark
                  .read
                  .schema(schema)
                  .option("multiline","true")
                  .json("clients.json")

    val cli_clean = clients.withColumn("id",col("id").cast(IntegerType))

    cli_clean.withColumn("even_id",col("id")*2).show()

    val subkey = List("address","city","state","zipcode") 
    val col_sub = subkey.map(x => col(s"location.$x")) 
    val col_init = cli_clean.columns.map(col(_))
    val cust_all = cli_clean.select(col_init ++ col_sub : _*).drop("location")

    cust_all.show()
  }
}