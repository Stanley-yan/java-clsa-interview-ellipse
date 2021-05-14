
public class MarketData {
	private String symbol;
	private long bid;
	private long last;
	private long ask;
	private long updateTime;
	
	public String getSymbol() {return symbol;}
	public void setSymbol(String symbol) { this.symbol = symbol;}
	
	public long getBid() {return bid;}
	public void setBid(long bid) { this.bid = bid;}

	public long getLast() {return last;}
	public void setLast(long last) { this.last = last;}

	public long getAsk() {return ask;}
	public void setAsk(long ask) { this.ask = ask;}

	public long getUpdateTime() {return updateTime;}
	public void setUpdateTime(long updateTime) { this.updateTime = updateTime;}

	public MarketData(String symbol,long bid,long last,long ask, long updateTime){
		this.symbol = symbol;
		this.bid = bid;
		this.last = last;
		this.ask = ask;
		this.updateTime = updateTime;
	}
	
	public String toString() {
		return this.symbol+"|"+this.bid+"|"+this.last+"|"+this.ask+"|"+this.updateTime;
	}
	
	
	
}
