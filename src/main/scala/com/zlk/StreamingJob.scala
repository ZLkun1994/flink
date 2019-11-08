/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zlk

import org.apache.flink.streaming.api.scala._
import java.util.Calendar

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
//    val text: DataStream[String] = env.socketTextStream("hadoop102",9999,'\n')
//    val windowCounts: DataStream[WordWithCount] = text.flatMap(w => w.split("\\s"))
//            .map(w => WordWithCount(w, 1))
//            .keyBy("w")
//            .timeWindow(Time.seconds(5))
//            .sum("i")
//    windowCounts.print().setParallelism(1)



//      val sensorIds: DataStream[String] = readings.map(new MyMapFunction)
//
//      class MyMapFunction extends MapFunction[SensorReading, String] {
//          override def map(r: SensorReading): String = r.id
//      }
//
        val readings: DataStream[SensorReading] = env.addSource(new SensorSource)

      // 报警
//      val warnings = readings
//              // key by sensor id
//              .keyBy(_.id)
//              // apply ProcessFunction to monitor temperatures
//              .process(new TempIncreaseAlertFunction)
//      warnings.print()
      //合并
//      val filterSwitches: DataStream[(String, Long)] = env.fromCollection(Seq(
//          ("sensor_2", 10 * 1000L),
//          ("sensor_7", 20 * 1000L)
//      ))
//      readings.connect(filterSwitches)
//                      .keyBy(_.id,_._1)
//                      .process(new ReadingFliter)
//                      .print()
      //求平均温度
//      readings.map(r => (r.id,r.temperature))
//              .keyBy(_._1)
//              .timeWindow(Time.seconds(5))
//              .aggregate(new AvgTempFunction)
//              .print()
      //求最大值最小值
//      readings
//              .keyBy(_.id)
//              .timeWindow(Time.seconds(5))
//          .process(new HighAndLowTempProcessFunction).print()
// 如果只使用ProcessWindowFunction(底层的实现为将事件都保存在ListState中)，将会非常占用空间。
//      readings
//              .map( r => (r.id,r.temperature,r.temperature))
//              .keyBy(_._1)
//              .timeWindow(Time.seconds(5))
//                      .reduce ( (r1: (String, Double, Double), r2: (String, Double, Double)) => (r1._1, r1._2.min(r2._2), r1._3.max(r2._3))
//                          ,new AssignWindowEndProcessFunction
//                      ).print()'
      //增量聚合算最大最小.
//      readings.map(r => (r.id,r.temperature, r.temperature))
//                      .keyBy(_._1)
//                      .timeWindow(Time.seconds(5))
//                      .aggregate(new MyAgg,new MyProcessFunction)
//                      .print()
/*
        val stream: DataStream[String] = env.socketTextStream("hadoop102",9999,'\n')
        stream.map(line => {
            val strings: Array[String] = line.split(" ")
            (strings(0).toString,strings(1).toLong * 1000)
        }).assignAscendingTimestamps(r => r._2).process(new LateFilter)
                        .print()
*/


    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * http://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

  case class WordWithCount(w: String, i: Int)
    // 传感器id，时间戳，温度
//    case class SensorReading(
//                                    id: String,
//                                    timestamp: Long,
//                                    temperature: Double
//                            )


    // 传感器id，时间戳，温度
    case class SensorReading(id: String, timestamp: Long, temperature: Double)

    // 需要extends RichParallelSourceFunction, 泛型为SensorReading
    class SensorSource extends RichParallelSourceFunction[SensorReading] {

        // flag indicating whether source is still running.
        // flag: 表示数据源是否还在正常运行
        var running: Boolean = true

        /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
        // run()函数连续的发送SensorReading数据，使用SourceContext
        // 需要override
        override def run(srcCtx: SourceContext[SensorReading]): Unit = {

            // initialize random number generator
            // 初始化随机数发生器
            val rand = new Random()
            // look up index of this parallel task
            // 查找当前运行时上下文的任务的索引
            val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

            // initialize sensor ids and temperatures
            // 初始化10个(温度传感器的id, 温度值)元组
            var curFTemp = (1 to 10).map {
                // nextGaussian产生高斯随机数
                i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
            }

            // emit data until being canceled
            // 无限循环，产生数据流
            while (running) {

                // update temperature
                // 更新温度
                curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
                // get current time
                // 获取当前时间戳
                val curTime = Calendar.getInstance.getTimeInMillis

                // emit new SensorReading
                // 发射新的传感器数据, 注意这里srcCtx.collect
                curFTemp.foreach( t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))

                // wait for 100 ms
                Thread.sleep(100)
            }

        }

        /** Cancels this SourceFunction. */
        // override cancel函数
        override def cancel(): Unit = {
            running = false
        }

    }
    //1s报警
    class TempIncreaseAlertFunction extends KeyedProcessFunction[String,SensorReading,String]{
        //lazy 运行时才开辟空间,也会导致一下问题,不知道什么时候能用到,不好排查bug
        //通过上下文, 获得上一个传感器的温度值,调用继承类的方法
        lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",Types.of[Double]))
        //保存注册定时器的时间戳
        lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",Types.of[Long]))
        //针对流里面的每一个元素
        //#Context类型投影,
        override def processElement(r: SensorReading,
                                    context: KeyedProcessFunction[String, SensorReading, String]#Context,
                                    collector: Collector[String]): Unit = {
            //取出上一次的温度
            val prevTemp: Double = lastTemp.value()
            //将温度更新到上一次的温度到这个变量中
            lastTemp.update(r.temperature)
            //获得之前设置定时器的时间戳
            val curTimerTimesamp: Long = currentTimer.value()
            if (prevTemp == 0.0 || r.temperature < prevTemp){
                //温度下降或者时第一温度值,删除定时器
                context.timerService().deleteProcessingTimeTimer(curTimerTimesamp)
                //清空状态
                currentTimer.clear()
            }else if (r.temperature > prevTemp && curTimerTimesamp == 0){
                //温度上升,我们不设置定时器
                val timerTs: Long = context.timerService().currentProcessingTime() + 1000
                //注册处理的定时时间
                context.timerService().registerProcessingTimeTimer(timerTs)
                //更新现实定时器
                currentTimer.update(timerTs)
            }

        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            out.collect("传感器id为:"+ ctx.getCurrentKey +"传感器的温度已经恋曲1s上升了")
            currentTimer.clear()
        }
    }

    /**
      *
      */
    class ReadingFliter extends CoProcessFunction[SensorReading,(String,Long),SensorReading]{

        lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("filterSwitch",Types.of[Boolean]))
        lazy val disbaleTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",Types.of[Long]))

        override def processElement1(in1: SensorReading,
                                     context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                     out: Collector[SensorReading]) {

            if (forwardingEnabled.value()){
                //如果为决定我们是否要将数据进行继续传下去
                out.collect(in1)
            }
        }

        override def processElement2(in2: (String, Long),
                                     context: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                     out: Collector[SensorReading]){
            //允许传输数去
            forwardingEnabled.update(true)
            //设置定时器时间
            val getTime: Long = context.timerService().currentProcessingTime() + in2._2
            val curTimerTimestamp : Long = disbaleTimer.value()
            if (getTime > curTimerTimestamp){
                context.timerService().deleteEventTimeTimer(curTimerTimestamp)
                //注册定时器，到时间出发任务
                context.timerService().registerProcessingTimeTimer(getTime)
                //更新触发器的值
                disbaleTimer.update(getTime)

            }
        }

        override def onTimer(timestamp: Long,
                             ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                             out: Collector[SensorReading]) {

            forwardingEnabled.clear()
            disbaleTimer.clear()
        }
    }
    //求平均值
    class AvgTempFunction extends AggregateFunction[(String,Double),(String,Double,Int),(String,Double)]{
        //创建累加器 ，初始化
        override def createAccumulator(): (String, Double, Int) = {
            ("",0.0,0)
        }
        override def add(in: (String, Double), acc: (String, Double, Int)): (String, Double, Int) = {
            (in._1,in._2 + acc._2,acc._3 + 1)
        }
        //输出的是平均值
        override def getResult(acc: (String, Double, Int)): (String, Double) = {
            (acc._1,acc._2/acc._3)
        }

        override def merge(acc: (String, Double, Int), acc1: (String, Double, Int)): (String, Double, Int) = {
            (acc._1,acc._2 + acc1._2, acc._3 + acc1._3)
        }
    }

    //求最大最小值
    case class MinMaxTemp(id : String, min : Double, max : Double, endsTs :Long)
    //输入输出的泛型,和key
    class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading,MinMaxTemp,String, TimeWindow]{
        override def process(key: String,
                             context: Context,
                             elements: Iterable[SensorReading],
                             out: Collector[MinMaxTemp]): Unit = {
            //转换要获取的数据的类型
            val temperature: Iterable[Double] = elements.map(_.temperature)
            // 获得窗口的结束时间
            val tw: Long = context.window.getEnd
            out.collect(
                new MinMaxTemp(key,temperature.min,temperature.max,tw)
            )
        }
    }
    //reduce实现
    class AssignWindowEndProcessFunction extends ProcessWindowFunction[(String,Double,Double),MinMaxTemp,String, TimeWindow]{
        override def process(key: String,
                             context: Context,
                             elements: Iterable[(String,Double,Double)],
                             out: Collector[MinMaxTemp]): Unit = {
            val minMax: (String,Double,Double) = elements.head
            val twEnd: Long = context.window.getEnd
            out.collect(MinMaxTemp(key,minMax._2,minMax._3,twEnd))
        }
    }
    //增量求最大最小
    class MyAgg extends AggregateFunction[(String,Double,Double),(Double,Double),(Double,Double)]{
        //存的时最大最小值
        override def createAccumulator(): (Double, Double) = (99999.0,0.0)

        override def add(in: (String, Double, Double), acc: (Double, Double)): (Double, Double) = {
            (in._2.min(acc._1),in._3.max(acc._2))
        }

        override def getResult(acc: (Double, Double)): (Double, Double) = acc

        override def merge(acc: (Double, Double), acc1: (Double, Double)): (Double, Double) = {
            (acc._1.min(acc1._1),acc._2.max(acc1._2))
        }
    }

    class MyProcessFunction extends ProcessWindowFunction[(Double,Double),MinMaxTemp,String, TimeWindow]{

        override def process(key: String,
                             context: Context,
                             elements: Iterable[(Double, Double)],
                             out: Collector[MinMaxTemp]): Unit = {
            val minMax: (Double,Double) = elements.head
            val twEnd: Long = context.window.getEnd
            out.collect(MinMaxTemp(key,minMax._1,minMax._2,twEnd))
        }
    }


    class LateFilter extends ProcessFunction[(String,Long),(String,Long)]{
        val late: OutputTag[(String, Long)] = new OutputTag[(String,Long)]("late")
        override def processElement(i: (String, Long),
                                    context: ProcessFunction[(String, Long), (String, Long)]#Context,
                                    collector: Collector[(String, Long)]): Unit = {
            //如果小于水位线就侧输出，否则就正常输出
            if (i._2 < context.timerService().currentWatermark()){
                context.output(late,i)
            }else{
                collector.collect(i)
            }
        }
    }

}
