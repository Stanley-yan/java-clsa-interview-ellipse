package marketDataProcessorTest;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import marketDataProcessor.IMessageListener;
import marketDataProcessor.MarketData;
import marketDataProcessor.MarketDataProcessor;
import nl.jqno.equalsverifier.EqualsVerifier;

public class MarketDataProcessTest {
    final List<String> symbols = Arrays.asList("A", "AAL", "AAP", "AAPL", "ABBV", "ABC", "ABMD", "ABT", "ACN", "ADBE", "ADI", "ADM", "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIV", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL", "ALLE", "ALXN", "AMAT", "AMCR", "AMD", "AME", "AMGN", "AMP", "AMT", "AMZN", "ANET", "ANSS", "ANTM", "AON", "AOS", "APA", "APD", "APH", "APTV", "ARE", "ATO", "ATVI", "AVB", "AVGO", "AVY", "AWK", "AXP", "AZO", "BA", "BAC", "BAX", "BBY", "BDX", "BEN", "BF", "BIIB", "BIO", "BK", "BKNG", "BKR", "BLK", "BLL", "BMY", "BR", "BRK", "BSX", "BWA", "BXP", "C", "CAG", "CAH", "CARR", "CAT", "CB", "CBOE", "CBRE", "CCI", "CCL", "CDNS", "CDW", "CE", "CERN", "CF", "CFG", "CHD", "CHRW", "CHTR", "CI", "CINF", "CL", "CLX", "CMA", "CMCSA");

    //  Ensure that the publishAggregatedMarketData method is not called any more than 100 times/sec where this period is a sliding window.
    @ParameterizedTest(name = "when {0} symbols with {1} update entries each and each symbol update received per 100MS, {2} publishing in total is expected")
    @CsvSource({"100,10,100", "101,10,100", "100,11,200", "101,11,200"})
    void publishAggregatedMarketDataMethodIsNotCalledAnyMoreThan100TimesPerSec(int noOfSymbols, int noOfSymbolRecords, int expectedTotalPublish) throws InterruptedException {
        List<MarketData> publishedEntries = new ArrayList<>();
        IMessageListener marketDataProcessor = new MarketDataProcessor(1000,100) {
            @Override
            public void publishAggregatedMarketData(MarketData data) {
                super.publishAggregatedMarketData(data);
                publishedEntries.add(data);
            }
        };
        final int msSleepTimeForEachSymbolRecord = 100;
        generateData(marketDataProcessor, symbols.stream().limit(noOfSymbols).collect(Collectors.toList()), msSleepTimeForEachSymbolRecord, noOfSymbolRecords);
        Assert.assertEquals(expectedTotalPublish, publishedEntries.size());
    }

    // Ensure each symbol will not have more than one update per second
    @ParameterizedTest(name = "when 3 symbols with {0} update entries each and each symbol update received per 100MS, {1} update entries publishing for each symbol is expected")
    @CsvSource({"0,0", "5,1", "11,2"})
    void eachSymbolWillNotHaveMoreThanOneUpdatePerSecond(int noOfUpdateEntriesForEachSymbol, int expectedNoOfPublishingForEachSymbol) throws InterruptedException {
        HashMap<String, Integer> publishCount = new HashMap<>();
        IMessageListener marketDataProcessor = new MarketDataProcessor(1000,100) {
            @Override
            public void publishAggregatedMarketData(MarketData data) {
                super.publishAggregatedMarketData(data);
                publishCount.put(data.getSymbol(), publishCount.getOrDefault(data.getSymbol(), 0) + 1);
            }
        };
        final int msSleepTimeForEachSymbolRecord = 100;
        List<String> inputSymbols = symbols.stream().limit(3).collect(Collectors.toList());
        generateData(marketDataProcessor, inputSymbols, msSleepTimeForEachSymbolRecord, noOfUpdateEntriesForEachSymbol);
        inputSymbols.forEach(s -> {
            Assert.assertEquals(expectedNoOfPublishingForEachSymbol, publishCount.getOrDefault(s, 0).intValue());
        });
    }

    // Ensure each symbol will always have the latest market data when it is published
    @Test
    void eachSymbolWillAlwaysHaveTheLatestMarketDataWhenItIsPublished() throws InterruptedException {
        HashMap<String, MarketData> publishedEntry = new HashMap<>();
        IMessageListener marketDataProcessor = new MarketDataProcessor(1000,100) {
            @Override
            public void publishAggregatedMarketData(MarketData data) {
                super.publishAggregatedMarketData(data);
                publishedEntry.put(data.getSymbol(), data);
            }
        };
        final int msSleepTimeForEachSymbolRecord = 100;
        final int noOfUpdateEntriesForEachSymbol = 11;
        List<String> inputSymbols = symbols.stream().limit(3).collect(Collectors.toList());
        generateData(marketDataProcessor, inputSymbols, msSleepTimeForEachSymbolRecord, noOfUpdateEntriesForEachSymbol);
        final double delta = 0.000001;
        inputSymbols.forEach(s -> {
            Assert.assertEquals(11, publishedEntry.get(s).getAsk(), delta);
            Assert.assertEquals(11, publishedEntry.get(s).getBid(), delta);
        });
    }    
    
    private void generateData(IMessageListener marketDataProcessor, List<String> symbols, int sleepTimeForEachSymbolRecord, int noOfSymbolRecords) throws InterruptedException {
        for (int i = 1; i <= noOfSymbolRecords; i++) {
            int finalI = i;
            for (String symbol : symbols) {
                marketDataProcessor.onMessage(new MarketData(symbol,finalI,finalI,finalI,LocalDateTime.now()));
            }
            Thread.sleep(sleepTimeForEachSymbolRecord);
        }
    }


}
