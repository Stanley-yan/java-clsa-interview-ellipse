package unittest;

import java.time.Instant;
import java.util.Random;

import marketDataProcessor.MarketData;
import marketDataProcessor.MarketDataProcessor;

public class Test_PublishLatestData {
	static Random rand;
	
	public static void main(String[] args) {
		System.out.println("Ensure PublishData frequency cannot exceed given threshold");
		MarketDataProcessor processor = new MarketDataProcessor(1000,100);
		rand = new Random();
		//1st step: make the window full
		for(int i = 0; i<120;i++) {
			processor.onMessage(new MarketData(
					String.valueOf(i),
					rand.nextInt(50),
					rand.nextInt(50),
					rand.nextInt(50),
					Instant.now().toEpochMilli()
					));
		}
		//2nd step: confirm the latest MarketData will publish
		for(int i = 100; i<50;i++) {
			processor.onMessage(new MarketData(
					String.valueOf(100),
					rand.nextInt(50),
					rand.nextInt(50),
					rand.nextInt(50),
					Instant.now().toEpochMilli()
					));
		}

	}
}
