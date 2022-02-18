package stage2

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by night on 4/4/17.
  */
object docTfIdf {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("docTfIdfIndex").setMaster("local")
    val sc = new SparkContext(conf)
    val txtPath = "/home/night/test/txt"
    val files = sc.objectFile[(String, String)](txtPath)
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

    //(word,(txt,weighted-tf))
    val tryword2 = tryword1.sortByKey().map { case ((word, txt), tf) => (word, (txt, 1 + math.log10(tf))) }

    //combine (word, idf) and (word, (txt,tf)) to get (word,txt,tfidf)
    val stage2_idfIndexPath = "/home/night/test/stage2/idfIndex"
    val idfIndex = sc.objectFile[(String, Double)](stage2_idfIndexPath)
    val combWord1 = tryword2.join(idfIndex)//(word,((txt,tf),idf)) type
    val combWord2 = combWord1.map(x=>{
      (x._2._1._1,(x._1, x._2._1._2 * x._2._2))
    })//(txt,(word,tfidf))

    //convert to (txt,(word, weighted-tfidf))
    val combWord3 = combWord2.groupByKey().sortByKey()//(txt, Iterable((word, tfidf))), this is used for computing similarity like vector
    val trylast = combWord3.map(t=>{
      var weight:Double = 0.0
      t._2.foreach(x => {
        weight += (x._2*x._2)
      })
      val wordMap = t._2.map(x=>{
        (x._1, x._2/weight)
      }).toMap
      (t._1, wordMap)
    })

    val stage2_docTfIdfIndexPath = "/home/night/test/stage2/docTfIdfIndex"
    trylast.saveAsTextFile(stage2_docTfIdfIndexPath)

  }
}
