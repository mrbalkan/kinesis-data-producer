package com.mrbalkan.KinesisDataProducer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.github.jsontemplate.JsonTemplate;

@SpringBootApplication
public class KinesisDataProducerApplication {

	private static String messageTemplate=null;
	private static int numberOfMessages;
	private static int cycleInterval;
	private static final int maxCycles = Integer.MAX_VALUE;
	private static KinesisProducer kinesisProducer =null;
	public static String kinesisDataStreamName;

	private static void produceLoop() {
		
		System.out.println("Starting producing messages. Interrupt with CTRL+C");

		//initialize producer
		KinesisProducerConfiguration config = new KinesisProducerConfiguration()
				.setRecordMaxBufferedTime(3000)
				.setMaxConnections(1)
				.setRequestTimeout(60000)
				.setRegion("us-east-1");
		kinesisProducer = new KinesisProducer(config);

		int cycle=0;
		while (cycle<maxCycles) {
			cycle++;
			try {
				sendBatch(numberOfMessages);
			} catch (UnsupportedEncodingException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}			
			try {
				Thread.sleep(cycleInterval*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static void sendBatch(int numberOfMessages) throws UnsupportedEncodingException, InterruptedException {
		List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>(); 
		for (int i = 0; i < numberOfMessages; ++i) {
			ByteBuffer data = ByteBuffer.wrap(new JsonTemplate(messageTemplate).prettyString().getBytes("UTF-8"));
			// asynch doesn't block.
			putFutures.add(kinesisProducer.addUserRecord(kinesisDataStreamName, "thisalwaysgoestosamepartition", data)); 
		}
		// Wait for puts to finish and check the results 
		for (Future<UserRecordResult> f : putFutures) {
			try {
				UserRecordResult result = f.get(); // this does block and client retries if there's a failure, UserRecordFailed is sent. 
				if (result.isSuccessful()) {         
					System.out.println("Put record into shard " + result.getShardId());     
				} else {
					for (Attempt attempt : result.getAttempts()) {
						// Analyze and respond to the failure 
						System.out.println("Attempt error response: " + attempt.getErrorMessage());
					}
				}
			}catch(ExecutionException e1) {
				System.out.println("Could not process put: " + e1.getLocalizedMessage());
			}
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(KinesisDataProducerApplication.class, args);
		if(args!=null && args.length>=3) {
			kinesisDataStreamName=args[0];
			numberOfMessages=Integer.parseInt(args[1]);
			cycleInterval=Integer.parseInt(args[2]);
			if(args.length>3) {
				Path path = Paths.get(args[3]);
				try {
					messageTemplate = Files.readAllLines(path).get(0);
				} catch (IOException e) {
					System.out.println("Invalid template path. Please check the template path parameter and try again.");
					System.exit(-1);
				}
			}
			else {
				//assign default template
				messageTemplate = "{" +
						"  id : @i(max=4)," +
						"  utilization : @i(max=100)," +
						"  type : @s[](min=1, max=3)," +
						"  location : {" +
						"    lattitude : @i" +
						"  }" +
						"}";
			}
		}
		else {
			System.out.println("Usage: KinesisDataProducerApplication <KINESIS_DATA_STREAM_NAME> <NUMBER_OF_MESSAGES_PER_CYCLE> <CYCLE_INTERVAL> <OPTIONAL:TEMPLATE_FILE_PATH>");
			System.exit(-1);
		}
		produceLoop();
	}
}