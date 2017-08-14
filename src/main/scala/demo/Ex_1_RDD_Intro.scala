package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

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

    val programmersProfiles = sc.parallelize(Seq(("Ivan", "Java"), ("Elena", "Scala"), ("Paetja", "Scala")))
    // programmersProfiles.saveAsSequenceFile(workingFolder + "programmersProfiles")

    println("Statistics of languages")
    sc.sequenceFile(workingFolder + "programmersProfiles",
        classOf[org.apache.hadoop.io.Text], // ключ был типа текст(хадуповский)
        classOf[org.apache.hadoop.io.Text]) // и значение
      .map{ case (x,y) => (x.toString, y.toString)} // pattern matching: преобр. в строки scal'ы
      .join(codeRows)
      .collect
      .foreach(println)

    programmersProfiles.cogroup(codeRows).sortByKey(false).collect.foreach(println)

    val anomalInts = sc.parallelize(Seq(1,1,1,1,1,2,2,2,2,3,3,150,1,13,3,3,-100,3,3,3,3,1,1,1))
    val stats = anomalInts.stats()
    val stdev = stats.stdev
    val mean = stats.mean
    print("Stdev = " + stdev + "  Mean = " + mean)

    val normalInts = anomalInts.filter(x => (math.abs(x-mean) < 3*stdev))
    normalInts.collect.foreach(println)

    val config = sc.broadcast(("order" -> 3, "filter" -> true)) // стала глобальной переменной
    println(config.value)

    val accum = sc.accumulator(0)
    val ints = sc.textFile(workingFolder + "ints")
    ints.cache()
    ints.foreach(x => accum.add(x.toInt))
    println(ints.toDebugString)
    println(accum.value)

    // аккумуляторы как правило тольдля отладки и тестирования
    val accumV2 = new LongAccumulator()
    sc.register(accumV2) // регистрация аккумулятора в спарк-контексте
    ints.foreach(x => accumV2.add(x.toLong))
    if (!accumV2.isZero) {
      println(accumV2.sum)
      println(accumV2.avg)
      println(accumV2.value)
      println(accumV2.count)
    }

    val accumV2copy = accumV2.copy()
    sc.register(accumV2copy)
    ints.take(5).foreach(x => accumV2copy.add(x.toLong))
    accumV2.merge(accumV2copy)
    println("Merged value " + accumV2copy.value)

  }
}
