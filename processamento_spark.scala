import org.apache.spark.sql.SparkSession
//import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object processamento{
	def main(args: Array[String]){
		//funcionalidades do Spark
		val spark = SparkSession.builder().appName("processamento_spark").getOrCreate();
		spark.sparkContext.setLogLevel("WARN")

		//leitura do arquivo csv - DataFrame
		val crypto_csv = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("/user/caioChiuchi/input/crypto_data.csv")
		val dolar_csv = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("/user/caioChiuchi/input/dolar_data.csv")

		dolar_csv.createOrReplaceTempView("dolarTemp")
		val dolar_df = spark.sql("SELECT last_value(value, true) as value, last_value(timestamp) as timestamp from dolarTemp")

		crypto_csv.createOrReplaceTempView("cryptoTemp")
		val crypto_df = spark.sql("SELECT code, symbol, name, priceUSD, priceBTC, change24H, volume24H, timestamp from cryptoTemp")

		val crypto_dolar = dolar_df.crossJoin(crypto_df.drop("timestamp"))

		val crypto_to_real : (Float, Float) => Float = (priceUSD:Float, value:Float) => {priceUSD * value}

		val convert_udf = udf(crypto_to_real)

		val result = crypto_dolar.withColumn("priceReal", convert_udf(crypto_dolar.col("value"),crypto_dolar.col("priceUSD")))

		val final_result = result.select("code", "symbol", "name", "priceUSD", "priceReal" , "priceBTC", "change24H", "volume24H", "timestamp")

		final_result.write.json("/user/caioChiuchi/output/processado_data.json")
		final_result.write.json("/user/caioChiuchi/output/transferidos/processado_data.json")
		System.exit(0)
	}
}
