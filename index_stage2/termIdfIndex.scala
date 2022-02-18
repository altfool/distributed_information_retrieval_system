package stage2

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by night on 4/4/17.
  */
object termIdfIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("idfIndex").setMaster("local")
    val sc = new SparkContext(conf)
    val txtPath = "/home/night/test/txt"
    val files = sc.objectFile[(String, String)](txtPath)
    val N = files.count()
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
    //(words,idf)
    val aggWords = sc.parallelize(words.countByKey().toList).repartition(4)
    val idfWords = aggWords.map(x=>{
      (x._1, math.log10(N/x._2))
    }).sortByKey()

    //    val aggWords = words.groupByKey().sortByKey()
    val stage2_idfIndexPath = "/home/night/test/stage2/idfIndex"
    idfWords.saveAsTextFile(stage2_idfIndexPath)
    sc.stop()
  }
}
