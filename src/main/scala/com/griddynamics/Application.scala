package griddynamics


import java.util
import java.util.{Arrays, Properties}

import com.griddynamics.objects.Mappers.{GLCountryLocationsEn, GlCountryIPv4, ProductPurchase}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.functions.udf

object Application extends App {

  val checkIfIPIsInRangeUDF = udf((ipAddress: String, network: String) => new SubnetUtils(network).getInfo.isInRange(ipAddress))
  val getAllIPByNetworkUDF = udf((network: String) => new SubnetUtils(network).getInfo.getAllAddresses.mkString(","))

  val top10CatTaskTableName = "most_frequently_purchased_categories_spark"
  val top10ProdInEachCatTaskTableName = "most_frequently_purchased_product_in_each_category_spark"
  val top10CountriesTaskTableName = "countries_with_the_highest_money_spending_spark"
  val top10CountriesTaskFilteredNetworkTableName = "countries_with_the_highest_money_spending_spark_fn"

  val conf = new SparkConf().setAppName("SparkExamTaskMaven").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)
  //conf.set("spark.sql.crossJoin.enabled", "true")
  //val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  import hiveContext.implicits._

  //val productCsvRDD = sc.textFile("src/main/resources/product_purchases2.csv")
  //val geoliteCountryIPv4CsvRDD = sc.textFile("src/main/resources/GeoLite2-Country-Blocks-IPv4.csv")
  //val geoliteCountryLocationCsvRDD = sc.textFile("src/main/resources/GeoLite2-Country-Locations-en.csv")
  val productCsvRDD = sc.textFile("hdfs:///user/ykovalenko/events/*/*/*/*")
  val geoliteCountryIPv4CsvRDD = sc.textFile("hdfs:///user/ykovalenko/hive/gl/country/ipv4/GeoLite2-Country-Blocks-IPv4.csv")
  val geoliteCountryLocationCsvRDD = sc.textFile("hdfs:///user/ykovalenko/hive/gl/country/location-en/GeoLite2-Country-Locations-en.csv")

  //top10MostFrequentlyPurchasedCategories()
  //top10MostFrequentlyPurchasedProductsInEachCategory()
  //top10CountriesWithTheHighestMoneySpending()
  top10CountriesWithTheHighestMoneySpendingFilteredNetwork()

  def top10MostFrequentlyPurchasedCategories() {

    val productRDD = productCsvRDD.map {
      line =>
        val col = line.split(",")
        ProductPurchase(col(0), col(1), col(2), col(3), col(4))
    }

    val top10CategoriesTempDF = productRDD
      .map(row => (row.category, 1))
      .reduceByKey((accumulated, value) => accumulated + value)
      .sortBy(row => (row._2, row._1), ascending = false)
      .toDF().limit(10) //.show()

    val top10CategoriesDF = top10CategoriesTempDF
        .select(
          col("_1").as("category"),
          col("_2").as("count")
        )

    writeDfToDB(top10CategoriesDF, top10CatTaskTableName)
  }

  def top10MostFrequentlyPurchasedProductsInEachCategory() {

    val productRDD = productCsvRDD.map {
      line =>
        val col = line.split(",")
        ProductPurchase(col(0), col(1), col(2), col(3), col(4))
    }


    val top10ProdutsInEachCategoryTempDF = productRDD.toDF()
      .groupBy(col("category"), col("brand"))
      .count().as("count")
      .withColumn("row_number",
        row_number().over(Window.partitionBy("category").orderBy(col("count").desc)))
      .where(col("row_number") <= 10).toDF()

    val top10ProdutsInEachCategoryDF = top10ProdutsInEachCategoryTempDF.select(
      col("category"),
      col("brand"),
      col("count")
    )


    writeDfToDB(top10ProdutsInEachCategoryDF, top10ProdInEachCatTaskTableName)
  }

  def top10CountriesWithTheHighestMoneySpending() {

    val productRDD = productCsvRDD.map {
      line =>
        val col = line.split(",")
        ProductPurchase(col(0), col(1), col(2), col(3), col(4))
    }

    val geoliteCountryIPv4RDD = geoliteCountryIPv4CsvRDD.map {
      line =>
        val col = line.split(",")
        GlCountryIPv4(col(0), col(1), col(2), col(3), col(4), col(5))
    }

    val geoliteCountryLocationRDD = geoliteCountryLocationCsvRDD.map {
      line =>
        val col = line.split(",")
        GLCountryLocationsEn(col(0), col(1), col(2), col(3), col(4), col(5), col(6))
    }


    val countryIPv4RDDByIdCountry = geoliteCountryIPv4RDD.map(ipv4 => (ipv4.geonameId, ipv4))
    val countryNameRDDByIdCountry = geoliteCountryLocationRDD.map(country => (country.geonameId, country))
    val countryNetworkTempRDD = countryIPv4RDDByIdCountry.join(countryNameRDDByIdCountry)
      .map(row => (row._2._1.network, row._2._2.countryName))
    //.toDF()
    //.show()

    val countryNetworkRDD = countryNetworkTempRDD.toDF().select(
      col("_1").as("network"),
      col("_2").as("country")
    )

    val productPurchaseWithNoQuotesDF = productRDD.toDF().select(
      regexp_replace(col("brand"), "\"", "").as("brand"),
      regexp_replace(col("price"), "\"", "").as("price"),
      regexp_replace(col("purchaseDate"), "\"", "").as("purchaseDate"),
      regexp_replace(col("category"), "\"", "").as("category"),
      regexp_replace(col("ip_address"), "\"", "").as("ip_address")
    )


    val productPurchaseDF = productPurchaseWithNoQuotesDF.as("productpurchase")
    val countryNetworkDF = countryNetworkRDD.toDF().as("countrynetwork")
    val productWithCountryDF = productPurchaseDF.join(countryNetworkDF,
      checkIfIPIsInRangeUDF(col("productpurchase.ip_address"), col("countrynetwork.network")))
      .select(
      col("price"),
      col("country")
      )

    val top10CountriesWithTheHighestMoneySpendingDF = productWithCountryDF.select(
      col("country").as("country_name"),
      col("price"))
      .groupBy(col("country_name"))
      .agg(sum(col("price")).as("sum"))
      .sort(desc("sum"))
      .limit(10)



    writeDfToDB(top10CountriesWithTheHighestMoneySpendingDF, top10CountriesTaskTableName)
  }

  def top10CountriesWithTheHighestMoneySpendingFilteredNetwork() {

    val productRDD = productCsvRDD.map {
      line =>
        val col = line.split(",")
        ProductPurchase(col(0), col(1), col(2), col(3), col(4))
    }

    val geoliteCountryIPv4RDD = geoliteCountryIPv4CsvRDD.map {
      line =>
        val col = line.split(",")
        GlCountryIPv4(col(0), col(1), col(2), col(3), col(4), col(5))
    }

    val geoliteCountryLocationRDD = geoliteCountryLocationCsvRDD.map {
      line =>
        val col = line.split(",")
        GLCountryLocationsEn(col(0), col(1), col(2), col(3), col(4), col(5), col(6))
    }


    val countryIPv4RDDByIdCountry = geoliteCountryIPv4RDD.map(ipv4 => (ipv4.geonameId, ipv4))
    val countryNameRDDByIdCountry = geoliteCountryLocationRDD.map(country => (country.geonameId, country))
    val countryNetworkTempRDD = countryIPv4RDDByIdCountry.join(countryNameRDDByIdCountry)
      .map(row => (row._2._1.network, row._2._2.countryName))
    //.toDF()
    //.show()

    val countryNetworkRDD = countryNetworkTempRDD.toDF().select(
      col("_1").as("network"),
      col("_2").as("country")
    )

    val productPurchaseWithNoQuotesDF = productRDD.toDF().select(
      regexp_replace(col("brand"), "\"", "").as("brand"),
      regexp_replace(col("price"), "\"", "").as("price"),
      regexp_replace(col("purchaseDate"), "\"", "").as("purchaseDate"),
      regexp_replace(col("category"), "\"", "").as("category"),
      regexp_replace(col("ip_address"), "\"", "").as("ip_address")
    )


    val productPurchaseDF = productPurchaseWithNoQuotesDF.as("productpurchase")
    val countryNetworkDF = countryNetworkRDD.toDF().as("countrynetwork")
    val reducedCountryNetworkRDD = productPurchaseDF.join(countryNetworkDF
      , regexp_extract(regexp_replace(col("productpurchase.ip_address"), "\"", ""), "(\\d+).(\\d+).(\\d+).(\\d+)", 1) ===
        regexp_extract(col("countrynetwork.network"), "(\\d+).(\\d+).(\\d+).(\\d+)/(\\d+)", 1))
      .select(
      col("ip_address"),
      col("network"),
        regexp_replace(col("country"), "\"", "").as("country")
      )
      //.show()

    val reducedCountryNetworkDF = reducedCountryNetworkRDD.toDF().as("reducedcountrynetwork")
    val productWithCountryDF = productPurchaseDF.join(reducedCountryNetworkDF,
      col("productpurchase.ip_address") === col("reducedcountrynetwork.ip_address")
    )
      .filter(checkIfIPIsInRangeUDF(col("productpurchase.ip_address"), col("reducedcountrynetwork.network")))
      .select(
      col("price"),
      col("country")
      )

    val top10CountriesWithTheHighestMoneySpendingDF = productWithCountryDF.select(
      col("country").as("country_name"),
      col("price"))
      .groupBy(col("country_name"))
      .agg(sum(col("price")).as("sum"))
      .sort(desc("sum"))
      .limit(10)


    writeDfToDB(top10CountriesWithTheHighestMoneySpendingDF, top10CountriesTaskFilteredNetworkTableName)
  }


  def writeDfToDB(dfToSave: DataFrame, tableName: String) {

    val connectionProperties = new Properties()
    connectionProperties.put("user", "ykovalenko")
    connectionProperties.put("password", "q4cFCEPUwK9Z")
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")


    dfToSave.write
        .mode(SaveMode.Overwrite)
        .jdbc(

          "jdbc:mysql://ip-10-0-0-21.us-west-1.compute.internal:3306/ykovalenko",
          tableName,
          connectionProperties)
  }

}
