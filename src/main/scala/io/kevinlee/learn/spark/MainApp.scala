package io.kevinlee.learn.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Kevin Lee
  * @since 2016-05-29
  */
object MainApp extends App {
  val sparkConf = new SparkConf().setAppName("Simple Application")
    .setMaster("local")
  val sc = new SparkContext(sparkConf)

  val rdd1 = sc.parallelize(
    List(
      List("1", "Kevin", "kevin@email"),
      List("2", "John", "john@email"),
      List("3", "Tom", "tom@email")
    )
  )

  print(
    s"""
       |rdd1.filter(_.head.toInt == 1).first:
       |${rdd1.filter(_.head.toInt == 1).first}
     """.stripMargin)

  val pattern = """([a-zA-Z]+[']?[a-zA-Z]*).*""".r

  def takeWords(x: String): String = x match {
    case pattern(word) => word
    case _ => x
  }

  def wordCountUsingReduceByKey(rdd: RDD[String],
                                collector: RDD[(String, Int)] => Seq[(String, Int)]): Seq[(String, Int)] =
    collector(
      (for {
        line <- rdd
        word <- line.split("[\\s]+")
        onlyWord = takeWords(word)
      } yield (onlyWord, 1)).reduceByKey(_ + _)
    )

  def wordCountUsingGroupBy(rdd: RDD[String],
                            collector: RDD[(String, Int)] => Seq[(String, Int)]): Seq[(String, Int)] =
    collector(
      (for {
        line <- rdd
        word <- line.split("[\\s]+")
        onlyWord = takeWords(word)
      } yield onlyWord).groupBy(identity).map { case (k, v) => (k, v.size) }
    )

  val textRdd = sc.textFile(getClass.getResource("/lorem-ipsum.txt").getPath).cache()

  val ten = 10
  println(
    s"""
       |word count ($ten):
       |------
       |${textRdd.flatMap(_.split("[\\s]+")).map(takeWords).map(word => (word, 1)).reduceByKey(_ + _).take(ten).mkString(", ")}
       |------
       |${textRdd.flatMap(_.split("[\\s]+")).map(takeWords).groupBy(identity).map { case (k, v) => (k, v.size) }.take(ten).mkString(", ")}
     """.stripMargin)

  20 to 100 by 20 map { howMany =>
    s"""
       |word count ($howMany):
       |------
       |${wordCountUsingReduceByKey(textRdd, _.take(howMany))}
       |------
       |${wordCountUsingGroupBy(textRdd, _.take(howMany))}
     """.stripMargin
  } foreach (println)

  println(
    s"""
       |word count (all):
       |------
       |${wordCountUsingReduceByKey(textRdd, _.collect)}
       |------
       |${wordCountUsingGroupBy(textRdd, _.collect)}
     """.stripMargin
  )

}
