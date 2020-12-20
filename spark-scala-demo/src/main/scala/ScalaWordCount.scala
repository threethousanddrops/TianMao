import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object ScalaWordCount {
   def main(args: Array[String]) {
     if (args.length < 3) {
       System.err.println("Usage: <input path> <output path> <skip path>")
       System.exit(1)
     }

     val conf = new SparkConf().setAppName("Scala_PopularStores")
     val sc = new SparkContext(conf)
     val input = sc.textFile(args(0))
     val skip = sc.textFile(args(2))

     val rdd1 = input.filter(x => x.split(",")(5).equals("1111"))
     val rdd2 = rdd1.filter(x => x.split(",")(6).equals("0")==false)
     val rdd3 = rdd2.filter(x => !skip.contains(x.split(",")(3)))

     val tmp = rdd3.map(x=>(x.split(",")(3),1)).reduceByKey(_+_)
     val result = tmp.map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1)).take(100)
     val out = sc.saveAsTextFile(args(1))
     sc.stop()
    }
}
