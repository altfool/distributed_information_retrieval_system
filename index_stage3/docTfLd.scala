package stage3

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by night on 4/5/17.
  */
object docTfLd {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("docTfLd").setMaster("local")
    val sc = new SparkContext(conf)
    val txtPath = "/home/night/test/txt"
    val files = sc.objectFile[(String, String)](txtPath)
    val N = files.count()
    //generate the rdd of ((word, txtid), tf)
    val words = files.flatMap(f => {
      val list = mutable.LinkedList[((String, String), Double)]()
      var temp = list
      val word = f._2.split("""[(\s*)|(\t*)]""").toList
      for (j <- 0 to word.length - 1) {
        val wordj = word(j)
        temp.next = mutable.LinkedList[((String, String), Double)](((wordj, f._1), 1.0))
        temp = temp.next
      }
      val list_end = list.drop(1)
      list_end
    })

    //((word,txt),tf)
    val tryword1 = words.sortByKey().reduceByKey(_ + _)

    //(txt,Iterable(word,tf))
    val tryword2 = tryword1.sortByKey().map { case ((word, txt), tf) => (txt, (word, tf)) }.groupByKey()

    //compute Lave i.e. the average length of doc
    val aggDocLength = sc.accumulator(0.0)
    tryword2.foreach(x=>{
      var docLength = 0.0
      x._2.foreach(t=>{
        docLength += t._2
      })
      aggDocLength += docLength
    })
    val avgDocLength = aggDocLength.value / N

    //(txt, lengthRatio, Map(word, tf))
    val trylast = tryword2.map(x=>{
      (x._1, x._2.toList.length / avgDocLength, x._2.toMap)
    })
    val stage3_docTfLdPath = "/home/night/test/stage3/docTfLd"
    trylast.saveAsTextFile(stage3_docTfLdPath)
  }
}
