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
		MarketDataProcessor processor = new MarketDataProcessor(1000,20);
		rand = new Random();
		new Thread() {
			public void run() {
				for(int i = 0; i<100;i++) {
					processor.onMessage(new MarketData(
							String.valueOf(rand.nextInt(5)),
							rand.nextInt(50),
							rand.nextInt(50),
							rand.nextInt(50),
							Instant.now().toEpochMilli()
							));
					processor.onMessage(new MarketData(
							String.valueOf(rand.nextInt(5)),
							rand.nextInt(50),
							rand.nextInt(50),
							rand.nextInt(50),
							Instant.now().toEpochMilli()
							));
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				Hashtable<String, Vector<MarketData>> discardData = processor.getDiscardDataHashMap();
				discardData.forEach((k,v)->{
					System.out.print("Symbol:"+k+"|");
					v.forEach(action -> System.out.println(action.toString()));
				});
			}
		}.start();		
	}
}
