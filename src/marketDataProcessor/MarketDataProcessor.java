package marketDataProcessor;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class MarketDataProcessor implements IMessageListener {
	private int window_unit_time; //The unit time of a Sliding Window
	private int window_threshold; //The maximum no. of call within a window unit time
	
	private Queue<String> queue; //Queue for data to publish
	private Timer window_clock; //Timer for reset the Window
	
	private AtomicInteger window_consume_quota; //Counter to limit Call per Window unit time
	private ExecutorService publishMarketDataThread; //ThreadPool to execute publish data
	private Map<String, Vector<MarketData>> marketDataHashMap; //Store the repeat data before publish
	private Map<String, Vector<MarketData>> discardDataHashMap;//Store the discard data after publish (store only, not further usage)
	
	public int getWindowUnitTime() {return window_unit_time;}
	public void setWindowUnitTime(int unit_time) {this.window_unit_time = unit_time;}
	public int getWindowThreshold() {return window_threshold;}
	public void setWindowThreshold(int threshold) {this.window_threshold = threshold;}
	public Map<String, Vector<MarketData>> getDiscardDataHashMap(){return discardDataHashMap;}

	public MarketDataProcessor(int unit_time, int threshold){
		this.window_unit_time = unit_time;
		this.window_threshold = threshold;
		window_consume_quota = new AtomicInteger(0);
		queue = new LinkedList<String>();
		marketDataHashMap = Collections.synchronizedMap(new Hashtable<String,Vector<MarketData>>());
		discardDataHashMap = Collections.synchronizedMap(new Hashtable<String,Vector<MarketData>>());
		window_clock = new Timer();
		
		window_clock.schedule(new TimerTask() {
			@Override
			public void run() {
				window_consume_quota.set(0);
				System.out.println("1s, window reset: "+ window_consume_quota);
			}
			
		}, 0,window_unit_time);
		
		publishMarketDataThread = Executors.newFixedThreadPool(5);
		
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
		//put into a hashmap
		onReceive(data);
		//Put into Queue
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

			//publish data
			publishAggregatedMarketData(targetData);
			
			//Clear up the out-dated data
			removeDiscardDatafromQueueAndStore(symbolData, symbol);
		}
		//else, no data, pass
	}
	
	
	//Put MarketData into hashmap when onMessage fire
	public Vector<MarketData> onReceive(MarketData data) {
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
				return temp2;
			}
			else {
				//update the hashmap
				try {
					temp.add(data);
					marketDataHashMap.put(data.getSymbol(), temp);
					return temp;
				}catch (NullPointerException ex) {
					temp = new Vector<MarketData>();
					temp.add(data);
					marketDataHashMap.put(data.getSymbol(), temp);
					return temp;
				}
			}
		}
		else {//not exists, new a Vector and store
			Vector<MarketData> temp = new Vector<MarketData>();
			temp.add(data);
			marketDataHashMap.put(data.getSymbol(), temp);
			return temp;
		}
	}

	//After publishData, reorganize the out-dated makret data.
	public Map<String, Vector<MarketData>> removeDiscardDatafromQueueAndStore(Vector<MarketData> symbolData, String symbol){
		//get the discardData from discard data hashmap by the symbol
		Vector<MarketData> discardVector = discardDataHashMap.get(symbol);
		//check if any discard data before
		if(discardVector != null) {
			try {
				//have data, add to the end
				discardVector.addAll(symbolData);
				//update
				discardDataHashMap.put(symbol, discardVector);
			} catch (NullPointerException ex) {
				discardVector = new Vector<MarketData>();
				discardVector.addAll(symbolData);
				discardDataHashMap.put(symbol, discardVector);
			}
		}
		else //no data before, just add
			discardDataHashMap.put(symbol, symbolData);

		
		return discardDataHashMap;
	}
}
