package demo

import org.apache.spark.sql.SparkSession

object Ex_1_RDD_Intro {

  def main(args: Array[String]): Unit = {

    val workingFolder = "/home/msa/dev/SparcTraining/out/"
    System.setProperty("hadoop.home.dir", "d:\\hadoop_home_dir");

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("RDD_Intro")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val ds = spark.range(1000)
    println(ds.count) // 1000
    println(ds.rdd.count) //1000

    // storing RDD as a "file"
// val array = 1 to 10 toArray
// val ints = sc.parallelize(array.reverse, 3) // RDD
// ints.saveAsTextFile(workingFolder + "ints")
// val programmersProfiles = sc.parallelize(Seq(("Ivan", "Java"), ("Elena", "Scala"), ("Paetja", "Scala")))
// programmersProfiles.saveAsSequenceFile(workingFolder + "programmersProfiles")

    // reading RDD
    val cachedInts = sc.textFile(workingFolder + "ints")
        .map(x => x.toInt)
      // кэширование - фиксация приготовленного RDD и файла
      // в виде готового набора данных в памяти
        .cache()

    val squares = cachedInts.map(x=> x*x) // это RDD
    // пока не сделали collect, все операции трансформации выполняются на стороне worker'ов
    squares.collect().foreach(println)

    val even = squares.filter(x => x%2 == 0) // это тоже RDD
    even.collect().foreach(println)

    even.setName("Even numbers")
    println("Name is " + even.name + "id is " + even.id)
    println(even.toDebugString) // инфа по RDD - сколько занимает в памяти, на диске...

    println("Total multiplication is " + even.reduce((a,b) => a*b))

    // summary of all array's elements
    val array1 = 1 to 5 toArray
    val summ = sc.parallelize(array1, 3).cache().reduce((a, b) => a+b)
    println(summ + "\n")

    // трансформация массива чисел в массив пар
    val groups = cachedInts.map(x => if(x % 2 == 0) {
      (0,x)
    } else {
      (1,x)
    })

    //
    println(groups.groupByKey.toDebugString) // ShuffledRDD - произошло перемешивание
    groups.groupByKey.collect.foreach(println)
    println(groups.countByKey) // частотный анализ

    println("\ncount " + groups.count)
    println("first " + groups.first)
    println("the first two ")
    groups.take(2).foreach(println)
    println("the first three ordered")
    groups.takeOrdered(3).foreach(println)

    val codeRows = sc.parallelize(Seq(("Ivan", 250), ("Elena", -15), ("Paetja", 50), ("Elena", 290)))
    println("\nFake statistic of code writing performance")
    codeRows.reduceByKey((a,b) => a+b).collect.foreach(println)

    println("groupping by name ")
    codeRows.groupByKey.collect.foreach(println)

    println("Statistics of languages")
    sc.sequenceFile(workingFolder + "programmersProfiles",
        classOf[org.apache.hadoop.io.Text], // ключ был типа текст(хадуповский)
        classOf[org.apache.hadoop.io.Text]) // и значение
      .map{ case (x,y) => (x.toString, y.toString)} // pattern matching: преобр. в строки scal'ы
      .join(codeRows)
      .collect
      .foreach(println)

  }
}
