package unittest;

import java.time.Instant;
import java.util.Hashtable;
import java.util.Random;
import java.util.Vector;

import marketDataProcessor.MarketData;
import marketDataProcessor.MarketDataProcessor;

public class Test_publishDataLimitTo100perSecond {
	
	static Random rand;
	
	public static void main(String[] args) {
		System.out.print("Ensure PublishData frequency cannot exceed given threshold");
		MarketDataProcessor processor = new MarketDataProcessor(1000,100);
		rand = new Random();
		new Thread() {
			public void run() {
				for(int i = 0; i<1000;i++) {
					processor.onMessage(new MarketData(
							String.valueOf(rand.nextInt(1000)),
							rand.nextInt(50),
							rand.nextInt(50),
							rand.nextInt(50),
							Instant.now().toEpochMilli()
							));
				}
				
//				Hashtable<String, Vector<MarketData>> discardData = processor.getDiscardDataHashMap();
//				discardData.forEach((k,v)->{
//					System.out.print("Symbol:"+k+"|");
//					v.forEach(action -> System.out.println(action.toString()));
//				});
			}
		}.start();		
	}
}
