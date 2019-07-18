package results

import org.apache.spark.rdd.RDD

object JedisAPP {
  //指标1
  def resoult01(rdd: RDD[(Int,List[Double])]): Unit ={
    rdd.foreachPartition(f=>{

    })
  }
}
