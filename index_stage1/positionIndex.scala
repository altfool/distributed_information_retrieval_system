package stage1

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by night on 4/3/17.
  */
object positionIndex {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PositionIndex").setMaster("local")
    val sc = new SparkContext(conf)
    val txtPath = "/home/night/test/txt"
    val files = sc.objectFile[(String, String)](txtPath)
    //generate the rdd of ((word, txtid), position)
    val words = files.flatMap(f => {
      val list = mutable.LinkedList[((String, String), String)]()
      var temp = list
        val word = f._2.split("""[(\s*)|(\t*)]""").toList
        for (j <- 0 to word.length - 1) {
          val wordj = word(j)
          val wordPosition = j.toString
          temp.next = mutable.LinkedList[((String, String), String)](((wordj, f._1), wordPosition))
          temp = temp.next
      }
      val list_end = list.drop(1)
      list_end
    })

//    //with rdd ((word, txtid), position), first we combine position of the same word with same txt,
//    //then we get ((word, txtid), pos1:pos2:pos3), the linking symbol is ':', you can change it as you like
//    val tryword1 = words.sortByKey().reduceByKey(_ + ":" + _)
//
//    //then reconstruct the rdd => (word, txtid->positions), it's (string, string),
//    // the linking symbol is '->', you can change it too
//    val tryword2 = tryword1.sortByKey().map { case ((word, txt), poss) => (word, txt + "->" + poss) }
//
//    //now we can reduceByKey(), that is reduce by word, we can combine the txtid->positions together of the same word
//    //then we get (word, txtid1->pos1:pos2;txtid2->pos1:pos2), the linking symbol is ';', you can change it as you like.
//    val trylast = tryword2.sortByKey().reduceByKey(_ + ";" + _).sortByKey()

    //((word,txt),Iterable(position))
    val tryword1 = words.groupByKey()
    //(word, (txt,Iterable(position)))
    val tryword2 = tryword1.map(x=>(x._1._1, (x._1._2, x._2)))
    //(word, Iterable(txtid, Iterable(position)))
    val trylast = tryword2.groupByKey()
    val stage1_positionIndexPath = "/home/night/test/stage1/positionIndex"
    trylast.saveAsObjectFile(stage1_positionIndexPath)
  }
}
