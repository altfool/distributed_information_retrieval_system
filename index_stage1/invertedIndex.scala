package stage1

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by night on 4/3/17.
  */
object invertedIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("invertedIndex").setMaster("local")
    val sc = new SparkContext(conf)
    val txtPath = "/home/night/test/txt"
    val files = sc.objectFile[(String, String)](txtPath)
    val words = files.flatMap(x => {
      val list = mutable.LinkedList[(String, String)]()
      var temp = list
      val word = x._2.split("""[(\s*)|(\t*)]""").toList.iterator
      while (word.hasNext) {
        temp.next = mutable.LinkedList[(String, String)]((word.next(), x._1))
        temp = temp.next
      }
      val list_end = list.drop(1)
      list_end
    }).distinct()
    /*
    //stop words
    ilter the most common and rare words
    val keyNumbers = sc.parallelize(words.countByKey().values.toList)
    val termMean = keyNumbers.mean()
    val stdDev = keyNumbers.sampleStdev()
    val minimum = termMean - 3 * stdDev
    val maximum = termMean + 3 * stdDev
    val newWords = aggWords.filter(x => {
     val num = x._2.split(";").length
     (num >= minimum) && (num <= maximum)
    })
    */
    //store it
    //(words, Iterable(txtid))
//    val aggWords = words.reduceByKey((a,b) => a + ";" + b).sortByKey()
    val aggWords = words.groupByKey().sortByKey()

    val stage1_InvertedIndexPath = "/home/night/test/stage1/invertedIndex"
    aggWords.saveAsObjectFile(stage1_InvertedIndexPath)
    sc.stop()
  }
}
