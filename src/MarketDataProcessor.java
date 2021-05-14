import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MarketDataProcessor {
	private int window_unit_time;
	private int window_threshold;
	private Queue<String> queue;
	private Timer window_clock;
	private AtomicInteger window_remain_quota;
	private ThreadPoolExecutor publishMarketDataThread;
	private Thread dequeue;
	private HashMap<String, ArrayList<MarketData>> marketDataHashMap;
	private HashMap<String, ArrayList<MarketData>> discardDataHashMap;
	
	public int getWindowUnitTime() {return window_unit_time;}
	public void setWindowUnitTime(int unit_time) {this.window_unit_time = unit_time;}
	public int getWindowThreshold() {return window_threshold;}
	public void setWindowThreshold(int threshold) {this.window_threshold = threshold;}

	MarketDataProcessor(int unit_time, int i){
		this.window_unit_time = unit_time;
		this.window_threshold = i;
		window_remain_quota = new AtomicInteger(i);
		queue = new LinkedList<String>();
		marketDataHashMap = new HashMap<String,ArrayList<MarketData>>();
		discardDataHashMap = new HashMap<String,ArrayList<MarketData>>();
		window_clock = new Timer();
		
		window_clock.schedule(new TimerTask() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				window_remain_quota.set(0);
				System.out.println("1s, window reset: "+ window_remain_quota);
			}
			
		}, 0,1000);
		
		this.publishMarketDataThread = new ThreadPoolExecutor(100,100,1,
				TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>());
		
		this.dequeue = new Thread() {
			public void run() {
				while(true) {
					String str;
					if(window_remain_quota.get()<window_threshold ) {
						if((str = queue.poll()) != null) {
							window_remain_quota.addAndGet(1);
							ArrayList<MarketData> temp  = marketDataHashMap.get(str);
							if(temp != null) {
								marketDataHashMap.remove(str);
								publishMarketDataThread.execute(()->{
									publishMarketDataWrapper(temp);
								});
							}
						}
					}
				}
			}
		};
		dequeue.start();

	}
	
	// Receive incoming market data
	public void onMessage(MarketData data) {
		//put into a queue 
		if(marketDataHashMap.containsKey(data.getSymbol())) {
			System.out.println("Same Data in HashMap:"+data.getSymbol());
			ArrayList<MarketData> temp = marketDataHashMap.get(data.getSymbol());
			temp.add(data);
		}
		else {
			ArrayList<MarketData> temp = new ArrayList<MarketData>();
			temp.add(data);
			marketDataHashMap.put(data.getSymbol(), temp);
		}
		
		queue.offer(data.getSymbol());
	}
	
	// Receive incoming market data
	public void onMessageTest(int data) {
		queue.offer(String.valueOf(data));
	}
	
	// Publish aggregated and throttled market data
	public void publishAggregatedMarketData(MarketData data) {
	// Do Nothing, assume implemented.
		System.out.println("Publish:"+data.getSymbol());
	}
	
	
	public void publishMarketDataWrapper(ArrayList<MarketData> symbolData) {
		
		MarketData targetData;
		//should be >0
		if(symbolData.size()>0) {
			targetData = symbolData.get(symbolData.size()-1);
			symbolData.remove(symbolData.size()-1);
			publishAggregatedMarketData(targetData);
			discardDataHashMap.put(targetData.getSymbol(), symbolData);
		}
	}
	
	public void publishInt(int i) {
		new Thread() {
			public void run() {
				System.out.println("Quota Remain:"+window_remain_quota+" Publish a int:"+i);
				//System.out.println("Queue:"+queue.toString());
			}
		}.start();
	}
}
