package stage1

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by night on 4/3/17.
  */
object biWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("biWordIndex").setMaster("local")
    val sc = new SparkContext(conf)
    val txtPath = "/home/night/test/txt"
    val files = sc.objectFile[(String, String)](txtPath)
    val words = files.flatMap(x => {
      val list = mutable.LinkedList[(String, String)]()
      var temp = list
      val word = x._2.split("""[(\s*)|(\t*)]""").toList.iterator
      var first = ""
      var second = ""
      if (word.hasNext){
        first = word.next()
      }
      while (word.hasNext) {
        second = word.next()
        temp.next = mutable.LinkedList[(String, String)]((first + " AND " + second, x._1))
        temp = temp.next
        first = second
      }
      val list_end = list.drop(1)
      list_end
    }).distinct()
    val stage1_biWordPath = "/home/night/test/stage1/biWord"
//    val aggWords = words.reduceByKey((a,b) => a + ";" + b).sortByKey()
//    aggWords.saveAsObjectFile(stage1_biWordPath)
//    (w1 AND w2, Iterable(txtid))
    words.groupByKey().sortByKey().saveAsObjectFile(stage1_biWordPath)
    sc.stop()
  }
}
