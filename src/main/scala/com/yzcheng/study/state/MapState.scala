package com.yzcheng.study.state

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object MapState {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /**
     * 使用MapState保存中间结果对下面数据进行分组求和
     * 1.获取流处理执行环境
     * 2.加载数据源
     * 3.数据分组
     * 4.数据转换，定义MapState,保存中间结果
     * 5.数据打印
     * 6.触发执行
     */
    val source: DataStream[(String, Int)] = env.fromCollection(List(
      ("java", 1),
      ("python", 3),
      ("java", 2),
      ("scala", 2),
      ("python", 1),
      ("java", 1),
      ("scala", 2)))

    source.keyBy(0)
      .map(new RichMapFunction[(String, Int), (String, Int)] {
        var mste: MapState[String, Int] = _

        override def open(parameters: Configuration): Unit = {
          val msState = new MapStateDescriptor[String, Int]("ms",
            TypeInformation.of(new TypeHint[(String)] {}),
            TypeInformation.of(new TypeHint[(Int)] {}))

          mste = getRuntimeContext.getMapState(msState)
        }
        override def map(value: (String, Int)): (String, Int) = {
          val i: Int = mste.get(value._1)
          mste.put(value._1, value._2 + i)
          (value._1, value._2 + i)
        }
      }).print()
    print("start")
    env.execute()
  }
}