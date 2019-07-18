package results

import java.lang

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{JedisConnectionPool, JedisOffset, SUtils}

/**
  * Redis管理Offset
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.testing.memory", "2147480000")
    val ssc = new StreamingContext(conf,Seconds(3))
    val provinceRDD = ssc.sparkContext.textFile("D:\\千锋dg3\\课件\\mixing\\项目\\手机充值实时处理\\充值平台实时统计分析\\city.txt")
    val provinceBroadCast = ssc.sparkContext.broadcast(provinceRDD.collect())

    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "zk002"
    // topic
    val topic = "hz1803b"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.60.101:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    stream.foreachRDD({
      rdd=>
        //rdd.foreachPartition( partition  => {
          val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          // 业务处理
          //rdd.map(_.value()).foreach(println)
          //将业务所需字段初步过滤处理提取到rdd并持久化
          val sUtils = new SUtils();
          val baseData = rdd.map(_.value()).map(t => JSON.parseObject(t))
            // 过滤需要的数据（充值通知）
            .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
            .map(t => {
              // 先判断一下充值结果是否成功
              val result = t.getString("bussinessRst") // 充值结果
              val money: Double = if (result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
              val feecount = if (result.equals("0000")) 1 else 0 // 充值成功数
              val starttime = t.getString("requestId") // 开始充值时间
              val stoptime = t.getString("receiveNotifyTime") // 结束充值时间
              val gettime = sUtils.parseTimestamp(stoptime) - sUtils.parseTimestamp(starttime.substring(0, 17))
              //充值时长
              val proid = t.getString("provinceCode")
              //省份ID
              val province = provinceBroadCast.value(proid.toInt) //省份
              (money, feecount, starttime, gettime, province)
            }).cache()
          //1.统计全网的充值订单量, 充值金额, 充值成功数,充值总时长
          baseData.map(t => {
            (1, List[Double](1, t._1, t._2, t._4))
          }).reduceByKey((list1, list2) => {
            list1.zip(list2).map(t => t._1 + t._2)
          }).foreach(print)

          /*.reduceByKey((list1,list2)=>{
          list1.zip(list2).map(t=>t._1+t._2)
        }).foreach(print)*/

          //2.实时充值业务办理趋势, 主要统计全网每分钟的订单量数据

          // 将偏移量进行更新
          val jedis = JedisConnectionPool.getConnection()
          for (or <- offestRange) {
            jedis.hset(groupId, or.topic + "-" + or.partition, or.untilOffset.toString)
          }
          jedis.close()
        //})
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
