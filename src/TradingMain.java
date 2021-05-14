import java.time.Instant;
import java.util.Random;

public class TradingMain {
	
	static Random rand;
	
	public static void main(String[] args) {
		MarketDataProcessor processor = new MarketDataProcessor(1000,20);
		rand = new Random();
		new Thread() {
			public void run() {
				for(int i = 0; i<200;i++) {
					processor.onMessage(new MarketData(
							String.valueOf(rand.nextInt(10)),
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
			}
		}.start();		
	}
}
