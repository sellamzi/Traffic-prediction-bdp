package example

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.RuntimeExecutionMode

object Hello {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    println("\n--- Flink batch program starting ---")

    val initialData: java.util.List[String] = java.util.Arrays.asList("hello", "world", "flink", "scala", "batch")
    val inputDataStream: DataStream[String] = env.fromCollection(initialData)
    val transformedDataStream: DataStream[String] = inputDataStream.map((value: String) => value.toUpperCase())
    
    transformedDataStream.print().name("Uppercase Words Sink")

    env.execute("hello-flink-batch")

    println("--- Flink batch program finished ---\n")
  }
}