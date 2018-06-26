package com.flinkexample;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// KeyedStreams
public class ex11_countWindow {

    public static void main(String[] args) throws Exception {
        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        if (dataStream == null) {
            System.exit(1);
            return;
        }

        DataStream<courseCount>outStream = dataStream
                .map(new parseRow())
                .keyBy("course")
                .countWindow(5)
                .sum("count");

        outStream.print();

        env.execute("Count Window");
    }

    public static class parseRow implements MapFunction<String, courseCount> {

        public courseCount map(String input) throws Exception {

            try {
                String[] rowData = input.split(",");

                return new courseCount(
                        rowData[0].trim(),
                        1);
            } catch (Exception ex) {
                System.out.println(ex);
            }

            return null;
        }


    }

    //    The class must be public
    //    It must have a public constructor without arguments
    //    All fields either have to be public or there must be getters and
    //    setters for all non-public fields. If the field name is foo the
    //    getter and setters must be called getFoo() and setFoo().
    public static class courseCount {
        public String course;
        public Integer count;

        public courseCount() {
        }

        public courseCount(String course, Integer count) {
            this.course = course;
            this.count = count;
        }

        public String toString() {
            return course + ": " + count;
        }
    }

}
