package marketDataProcessor;

public interface IMessageListener {
	void onMessage(MarketData marketData);
}
