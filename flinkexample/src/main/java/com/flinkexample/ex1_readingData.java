package com.flinkexample;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// FlatMap
public class ex1_readingData {

    public static void main(String[] args) throws Exception {
        // Checking input parameters -
        final ParameterTool params = ParameterTool.fromArgs(args);

        //execution edilebilecek ortamların oluşturulması
        final StreamExecutionEnvironment env = 
            StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = null;

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);


        if (params.has("input")) {

            //env.readTextFile -> yüklenecek yada oluşturulacak datanın kaynağı
            dataStream = env.readTextFile(params.get("input"));
        } else if (params.has("host") && params.has("port")) {

            //env.socketTextStream -> yüklenecek yada oluşturulacak datanın kaynağı
            dataStream = env.socketTextStream(
                    params.get("host"), Integer.parseInt(params.get("port")));
        } else {
            System.out.println("Use --host and --port to specify socket");
            System.out.println("Use --input to specify file input");
        }

        if(dataStream==null){
            System.exit(1);
            return;
        }

        dataStream.print(); // datayı sink etmek gibi düşünülebilir.

        // dış bir dosyaya kaynağı yazma işlemi
        dataStream.writeAsCsv(params.get("output"),
                FileSystem.WriteMode.OVERWRITE,
                "\n",
                ",");

        // trigger execution
        env.execute("Read and Write");
    }

}
