import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 游戏App相似度计算
  */
object GameAppSimilaritySqlRowNumber {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: <orcFileInputPath> <similarityFileOutputPath>")
      return
    }
    val orcFileInputPath: String = args(0)
    val similarityFileOutputPath: String = args(1)


    val classes: Array[Class[_]] = Array(classOf[collection.Map[String, Long]])

    //        val conf = new SparkConf().setAppName("GameAppSimilarity").setMaster("local[*]")
    val conf = new SparkConf().setAppName("GameAppSimilarity")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.registerKryoClasses(classes)
    // 序列化时使用的内存缓冲区大小
    conf.set("spark.kryoserializer.buffer.max", "128m")
    // 启用rdd压缩
    conf.set("spark.rdd.compress", "true")
    // 设置压缩格式为lz4, 默认也就是lz4, 这种压缩格式压缩比高, 速度快, 但是耗费的内存相对也多一些
    conf.set("spark.io.compression.codec", "snappy")
    // 设置压缩时使用的内存缓冲区大小
    conf.set("spark.io.compression.snappy.blockSize", "64k")
    // spark sql 在shuffle时产生的partition数量, 200和120,80效果差不多
    conf.set("spark.sql.shuffle.partitions", "280")
    // rdd默认的并行度
    conf.set("spark.default.parallelism", "280")
    // SortShuffleManager开启by-pass(不需要排序)模式的阈值, 默认为200, 在partition数量小于这个值时会开启by-pass模式
    conf.set("spark.shuffle.sort.bypassMergeThreshold", "300")
    // 网络连接相应超时时间
    conf.set("spark.network.timeout", "300s")
    // 数据本地化等待时间
    conf.set("spark.locality.wait", "10s")
    // shuffle溢写缓冲区, 默认32k, 在内存充足的情况下可以适当增加
    conf.set("spark.shuffle.file.buffer", "64k")
    // shuffle read task的buffer缓冲大小, 这个缓冲区大小决定了read task每次能拉取的数据量, 在内存充足的情况下可以适当增加
    conf.set("spark.reducer.maxSizeInFlight", "96m")

    // excutor 内存比例, 但是这个配置已经过时, 这里并不起作用, 如果需要调整这种比例, 需要先开启spark.memory.useLegacyMode配置
    // As of Spark 1.6, execution and storage memory management are unified
    //    conf.set("spark.storage.memoryFraction", "0.5")

    val builder: SparkSession.Builder = SparkSession.builder().config(conf).enableHiveSupport()
    val session: SparkSession = builder.getOrCreate()

    // 读取orc文件(数据源文件)
    // 这里是不是应该要进行缓存呢?
    // 事实证明, 并不需要进行持久化, 持久化了并不起作用
    val sourceDF: DataFrame = session.read.orc(orcFileInputPath)
    //.persist(StorageLevel.MEMORY_ONLY_SER)
    // 将加载的orc文件注册成临时表
    sourceDF.createOrReplaceTempView("sum_app_run_all")
    //    session.sqlContext.cacheTable("sum_app_run_all")

    // 先过滤掉运行游戏类app数量大于50和小于等于1的aid数据

    // 对数据进行过滤, 只需要游戏app的数据, 并且各个字段的内容不为空
    val userPkgInfoDF: DataFrame = session.sql(
      s"select " +
        s"a.aid, concat_ws('${content.MyContent.FILED_SEPARATOR}',pkgname,uptime) " +
        s"from " +
        s"(select * from sum_app_run_all where gp REGEXP '^game_') a " +
        s"inner join " +
        s"(select c.aid from (select aid, count(1) n from sum_app_run_all where gp REGEXP '^game_' group by aid) c where c.n < 50 and c.n > 1) b " +
        s"on a.aid = b.aid " +
        s"where pkgname is not null and uptime is not null"
    )

    // 转成rdd进行处理
    val rdd = userPkgInfoDF.rdd

    val reduceByKeyRDD: RDD[(String, String)] = rdd.mapPartitions(iter => {
      val res = new ArrayBuffer[(String, String)]()
      iter.foreach(row => {
        // 将row转换成(aid, pkgname\001uptime)对偶
        res.+=((row.getString(0), row.getString(1)))
      })
      res.iterator
    }).reduceByKey((str1, str2) => {
      // aid下所有app信息的聚合操作
      // 将同一个aid的所有app数据进行拼接
      // 拼接数据的格式为: pkgname1\001uptime1\002pkgname2\001uptime2\002pkgname3....
      str1 + content.MyContent.INFO_SEPARATOR + str2
    })


    // 创建累加器, 跟踪中间结果数据条数
    val intermediate = session.sparkContext.longAccumulator

    val twoAppSingleValue: RDD[(String, Double)] = reduceByKeyRDD.flatMap(info => {
      // 将aid下所有的app数据进行拆分, 生成一个数组
      val pkgInfos: Array[String] = info._2.split(content.MyContent.INFO_SEPARATOR)
      val sd: Double = 1 / math.log10(1 + pkgInfos.length)

      //      val res = new StringBuilder
      val tuples = new ArrayBuffer[(String, Double)]

      // 将app进行两两匹配, 计算单次的相似度贡献值
      for (i <- 0 until pkgInfos.length - 1) {
        val pkg1Infos = pkgInfos(i).split(content.MyContent.FILED_SEPARATOR)
        val pkg1Name = pkg1Infos(0)
        val pkg1Uptime = pkg1Infos(1)

        val start: Int = i + 1
        for (j <- start until pkgInfos.length) {
          val pkg2Infos = pkgInfos(j).split(content.MyContent.FILED_SEPARATOR)
          val pkg2Name = pkg2Infos(0)
          val pkg2Uptime = pkg2Infos(1)

          val td: Double = 1.0 / (1 + math.abs(pkg1Uptime.toLong - pkg2Uptime.toLong))
          // (a, b) (b, a),是一样的，排下序，防止重复
          val keyName: String = if (pkg1Name.compareTo(pkg2Name) < 0)
              pkg1Name + content.MyContent.PKGNAME_SEPARATOR + pkg2Name
            else
              pkg2Name + content.MyContent.PKGNAME_SEPARATOR + pkg1Name

          // 将两个app信息生成的单词相似度贡献值组合成一个对偶, 存入到数组中, flatmap操作会将这个数组拆分开
          intermediate.add(1)
          tuples.+=((keyName, sd * td))

          //          val similarity = sd * td
          //          // 直接进行数据冗余
          //          tuples.+=((pkg1Name + content.MyContent.PKGNAME_SEPARATOR + pkg2Name, similarity))
          //          tuples.+=((pkg2Name + content.MyContent.PKGNAME_SEPARATOR + pkg1Name, similarity))
        }
      }
      tuples
    })

    // 创建广播变量, 这个广播变量是一个Map, 这个Map中保存了每个pkgname对应的运行总次数
    // 这个Map的格式: (pkgname -> count)

    // 过滤掉那些运行游戏app数量大于50的用户安装的游戏app, 统计剩下的游戏app对应的app运行次数
    val pkgNumDF: DataFrame = session.sql("" +
      "select pkgname, count(1) num " +
      "from " +
      "(select a.aid, a.pkgname from (select * from sum_app_run_all where gp REGEXP '^game_') a " +
      "left join " +
      "(select c.aid from (select aid, count(1) n from sum_app_run_all where gp REGEXP '^game_' group by aid) c where c.n > 50) b " +
      "on a.aid = b.aid where b.aid is null" +
      ") d " +
      "group by pkgname")

    // 生成上述格式的Map对象, 这里直接使用的.toMap方法, 生成的是HashTrieMap对象,
    // 或许手动使用HashMap来构建这个Map对象,在后面的执行中效率是不是会更高?
    val pkgNumMap: collection.Map[String, Long] = pkgNumDF.rdd.mapPartitions(iter => {
      val pkgNumArr = new mutable.ArrayBuffer[(String, Long)]()
      iter.foreach(row => {
        pkgNumArr.+=((row.getString(0), row.getLong(1)))
      })
      pkgNumArr.iterator
    }).collectAsMap()

    // 对这个配置进行广播
    val pkgNumBroadcast: Broadcast[collection.Map[String, Long]] = session.sparkContext.broadcast(pkgNumMap)

    // 对每组pkgname生成的相似度单次贡献值进行累加, 得到总值
    val twoAppSumValue: RDD[(String, Double)] = twoAppSingleValue.reduceByKey(_ + _)

    // 对每组pkgname的相似度贡献值总值进行降权处理(对热门app进行惩罚)
    val pkgSimilarityRDD: RDD[(String, String, Double)] = twoAppSumValue.mapPartitions(pairs => {
      val resArr = new ArrayBuffer[(String, String, Double)]()
      pairs.foreach(info => {
        val pkgnames: Array[String] = info._1.split(content.MyContent.PKGNAME_SEPARATOR)
        val pkg1Num: Long = pkgNumBroadcast.value(pkgnames(0))
        val pkg2Num: Long = pkgNumBroadcast.value(pkgnames(1))
        val similarity: Double = math.log10(info._2 / math.sqrt(pkg1Num * pkg2Num))

        resArr.+=((pkgnames(0), pkgnames(1), similarity))
        resArr.+=((pkgnames(1), pkgnames(0), similarity))
      })
      resArr.iterator
    })

    import session.implicits._
    val runSimilarity: DataFrame = pkgSimilarityRDD.toDF("pkg1Name", "pkg2Name", "value")
    runSimilarity.createOrReplaceTempView("app_run_similarity")

    // 由于pkg1=>pkg2和pkg2=>pkg1的相似度应该是相同的, 所以这里做一个pkgname顺序颠倒的冗余
    val similarityRank: DataFrame = session.sql(
      "select pkg1name,pkg2name,value,row_number() OVER (DISTRIBUTE BY pkg1name SORT BY value DESC) rn from app_run_similarity")

    // 将最终的结果存成orc文件, 之后创建hive表来直接进行分析
    similarityRank.write.mode(SaveMode.Overwrite).orc(similarityFileOutputPath)

    // 打印累加器值
    println(s"intermediate =====> ${intermediate.count}")
  }
}
