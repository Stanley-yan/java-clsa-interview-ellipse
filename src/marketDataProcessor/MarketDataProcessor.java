package marketDataProcessor;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

interface onMessageCallBack{
	void onMessage(MarketData marketData);
};

public class MarketDataProcessor implements onMessageCallBack {
	private int window_unit_time; //The unit time of a Sliding Window
	private int window_threshold; //The maximum no. of call within a window unit time
	
	private Queue<String> queue; //Queue for data to publish
	private Timer window_clock; //Timer for reset the Window
	
	private AtomicInteger window_consume_quota; //Counter to limit Call per Window unit time
	private ThreadPoolExecutor publishMarketDataThread; //ThreadPool to execute publish data

	private Hashtable<String, Vector<MarketData>> marketDataHashMap; //Store the repeat data before publish
	private Hashtable<String, Vector<MarketData>> discardDataHashMap;//Store the discard data after publish (store only, not further usage)
	
	public int getWindowUnitTime() {return window_unit_time;}
	public void setWindowUnitTime(int unit_time) {this.window_unit_time = unit_time;}
	public int getWindowThreshold() {return window_threshold;}
	public void setWindowThreshold(int threshold) {this.window_threshold = threshold;}
	public Hashtable<String, Vector<MarketData>> getDiscardDataHashMap(){return discardDataHashMap;}

	public MarketDataProcessor(int unit_time, int threshold){
		this.window_unit_time = unit_time;
		this.window_threshold = threshold;
		window_consume_quota = new AtomicInteger(threshold);
		queue = new LinkedList<String>();
		marketDataHashMap = new Hashtable<String,Vector<MarketData>>();
		discardDataHashMap = new Hashtable<String,Vector<MarketData>>();
		window_clock = new Timer();
		
		window_clock.schedule(new TimerTask() {
			@Override
			public void run() {
				window_consume_quota.set(0);
				System.out.println("1s, window reset: "+ window_consume_quota);
			}
			
		}, 0,window_unit_time);
		
		//Only 100 thread, each thread last for 1s only.
		this.publishMarketDataThread = new ThreadPoolExecutor(100,100,1,
				TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>());
		
		
		new Thread() {
			public void run() {
				//Dequeue and control call
				while(true) {
					String str;
					//Check if still got quota to publish
					if(window_consume_quota.get()<window_threshold ) {
						//check if data exist in the Queue 
						if((str = queue.poll()) != null) {
							//Able to get a Data, consume a publish quota
							window_consume_quota.addAndGet(1);
							//get the data from Hashmap search by the queue key
							Vector<MarketData> marketDataVector  = marketDataHashMap.get(str);
							//should be able to get the Vector
							if(marketDataVector != null) {
								//remove the vector Hashmap, and publish, all newly incoming will put in to queue again
								marketDataHashMap.remove(str);
								publishMarketDataThread.execute(()->{
									publishMarketDataWrapper(marketDataVector);
								});
							}
							//else do nothing, but should not exists
						}
						//else no data in queue
					}
					//else do nothing and wait
				}
			}
		}.start();
		

	}
	
	// Receive incoming market data
	public void onMessage(MarketData data) {
		//put into a queue
		
		//1st check if the MarketData is exist in the Hashmap 
		if(marketDataHashMap.containsKey(data.getSymbol())) {
			//Exists data, get the Vector and then append to the end
			Vector<MarketData> temp = marketDataHashMap.get(data.getSymbol());
			//double check null condition
			if(temp == null) {
				Vector<MarketData> temp2 = new Vector<MarketData>();
				temp2.add(data);
				//update the hashmap
				marketDataHashMap.put(data.getSymbol(), temp2);
			}
			else {
				//update the hashmap
				temp.add(data);
				marketDataHashMap.put(data.getSymbol(), temp);
			}
		}
		else {//not exists, new a Vector and store
			Vector<MarketData> temp = new Vector<MarketData>();
			temp.add(data);
			marketDataHashMap.put(data.getSymbol(), temp);
		}
		
		
		//Put into Queue, can this line go to top?
		queue.offer(data.getSymbol());
	}
	

	// Publish aggregated and throttled market data
	public void publishAggregatedMarketData(MarketData data) {
	// Do Nothing, assume implemented.
		System.out.println("Publish:"+data.getSymbol());
	}
	
	
	//Pick the latest data to publish and move the unused data to another hashmap
	public void publishMarketDataWrapper(Vector<MarketData> symbolData) {
		
		MarketData targetData;
		String symbol;
		//should be >0
		if(symbolData.size()>0) {
			//get the last data, should be the latest
			targetData = symbolData.get(symbolData.size()-1);
			
			//get the corresponding symbol;
			symbol = targetData.getSymbol();
			
			//remove the latest data from Vector to publish 
			symbolData.remove(symbolData.size()-1);
			
			//publish data
			publishAggregatedMarketData(targetData);
			
			//get the discardData from discard data hashmap by the symbol
			Vector<MarketData> discardVector = discardDataHashMap.get(symbol);
			//check if any discard data before
			if(discardVector != null) {
				//have data, add to the end
				discardVector.addAll(symbolData);
				//update
				discardDataHashMap.put(targetData.getSymbol(), discardVector);
			}
			else //no data before, just add
				discardDataHashMap.put(targetData.getSymbol(), symbolData);
		}
		//else, no data, pass
	}
}
