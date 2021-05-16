package marketDataProcessor;

import java.time.LocalDateTime;

public class MarketData {
	private String symbol;
	private double bid;
	private double last;
	private double ask;
	private LocalDateTime updateTime;
	
	public String getSymbol() {return symbol;}
	public void setSymbol(String symbol) { this.symbol = symbol;}
	
	public double getBid() {return bid;}
	public void setBid(long bid) { this.bid = bid;}

	public double getLast() {return last;}
	public void setLast(long last) { this.last = last;}

	public double getAsk() {return ask;}
	public void setAsk(long ask) { this.ask = ask;}

	public LocalDateTime getUpdateTime() {return updateTime;}
	public void setUpdateTime(LocalDateTime updateTime) { this.updateTime = updateTime;}

	public MarketData(String symbol,long bid,long last,long ask, LocalDateTime updateTime){
		this.symbol = symbol;
		this.bid = bid;
		this.last = last;
		this.ask = ask;
		this.updateTime = updateTime;
	}
	
	public String toString() {
		return this.symbol+"|"+this.bid+"|"+this.last+"|"+this.ask+"|"+this.updateTime.toString();
	}
}
