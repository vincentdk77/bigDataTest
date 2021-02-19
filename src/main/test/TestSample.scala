import org.apache.spark.sql.SparkSession

object TestSample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val  entIdTupleRDD = spark.sparkContext.makeRDD(Array(("a",1),("a",2),("a",3),("b",2),("c",2),("c",2),("a",1),("c",2),("c",3),("c",1),("c",2),("c",3),("b",2),("b",2),("c",2)))
    val sampleRDD = entIdTupleRDD.sample(false, 0.8)
    val countMap = sampleRDD.countByKey()
    countMap.toList.sortWith(_._2 >_._2).take(10).foreach(println(_))
    println("===========================")

//    sampleRDD.collect().foreach(println(_))
//    val countMap: collection.Map[String, Long] = sampleRDD.countByKey()
//    countMap.foreach(println(_))
//    countMap.toList.sortWith(_._2 >_._2).take(20).foreach(println(_))

  }

}
