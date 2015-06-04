package com.cloudera.ingestion;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

public class DataIngestionKafkaConsumer
{
    private static final String HDFS_URL = "hdfs://ashwin-centos65-1.vpc.cloudera.com:8020";

    private static final String HDFS_BASE_DIR = "/user/ingestdata";
    
    private static final long BATCH_DURATION_MS = 10000;

    public static void main( String[] args )
    {
        if ( args.length < 4 )
        {
            System.err.println( "Usage: DataIngestionKafkaConsumer <zkQuorum> <group> <topics> <numThreads>" );
            System.exit( 1 );
        }

        // Configure the Streaming Context
        SparkConf sparkConf = new SparkConf().setAppName( "DataIngestionKafkaConsumer" );

        JavaStreamingContext ssc = new JavaStreamingContext( sparkConf, new Duration( BATCH_DURATION_MS ) );

        int numThreads = Integer.parseInt( args[3] );
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split( "," );
        for ( String topic : topics )
        {
            topicMap.put( topic, numThreads );
        }

        JavaPairReceiverInputDStream<String, String> messages =
            KafkaUtils.createStream( ssc, args[0], args[1], topicMap );

        System.out.println( "Got my DStream! connecting to zookeeper " + args[0] + " group " + args[1] + " topics"
            + topicMap );

        JavaDStream<String> lines = messages.map( new Function<Tuple2<String, String>, String>()
        {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;

            public String call( Tuple2<String, String> tuple2 )
            {
                return tuple2._2();
            }
        } );

        try
        {
            FileSystem hdfs = FileSystem.get( new URI( HDFS_URL ), new Configuration() );
            Path dirPath = new Path( HDFS_BASE_DIR );
            if ( !hdfs.exists( dirPath ) )
            {
                hdfs.mkdirs( dirPath );
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        catch ( URISyntaxException e )
        {
            e.printStackTrace();
        }

        final long timeFrame = System.currentTimeMillis();
        
        Function<JavaRDD<String>, Void> writeToHDFSFunc = new Function<JavaRDD<String>, Void>()
        {

            private static final long serialVersionUID = 1L;

            public Void call( JavaRDD<String> inLine )
                throws Exception
            {
                if ( inLine.count() > 0 )
                {
                    System.out.println( "******To be written to HDFS 1 " + inLine.first() );
                    inLine.saveAsTextFile( HDFS_URL + HDFS_BASE_DIR + "/file" + timeFrame );
                }
                else
                {
                    System.out.println( "No data to write at this timeframe" );
                }
                return null;
            }
        };
        lines.foreachRDD( writeToHDFSFunc );

        ssc.start();
        ssc.awaitTermination();

    }

}
