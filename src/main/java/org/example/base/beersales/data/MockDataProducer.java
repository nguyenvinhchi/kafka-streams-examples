package org.example.base.beersales.data;

import static org.example.base.stock.Topics.CLIENTS;
import static org.example.base.stock.Topics.FINANCIAL_NEWS;
import static org.example.base.zmart.data.generators.DataGenerator.NUM_ITERATIONS;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.base.beersales.Topics;
import org.example.base.beersales.data.generators.DataGenerator;
import org.example.base.beersales.model.BeerPurchase;
import org.example.base.stock.data.generators.Customer;
import org.example.base.stock.model.PublicTradedCompany;
import org.example.base.stock.model.StockTickerData;
import org.example.base.stock.model.StockTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockDataProducer {
    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);
    public static final int GENERATOR_DELAY_MS = 6000;
    public static final String STOCK_TICKER_TABLE_TOPIC = "stock-ticker-table";
    public static final String STOCK_TICKER_STREAM_TOPIC = "stock-ticker-stream";
    private static Producer<String, String> producer;
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static Callback callback;

    private static volatile boolean keepRunning = true;
    private static volatile boolean producingIQData = false;

    private static void init() {
        if (producer == null) {
            LOG.info("Initializing the producer");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "1");
            properties.put("retries", "3");

            producer = new KafkaProducer<>(properties);

            callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            LOG.info("Producer initialized");
        }
    }

    public static void produceStockTickerData() {
        produceStockTickerData(DataGenerator.NUMBER_TRADED_COMPANIES, NUM_ITERATIONS);
    }

    public static void produceStockTickerData(int numberCompanies, int numberIterations) {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            List<PublicTradedCompany> publicTradedCompanyList = DataGenerator.stockTicker(numberCompanies);

            while (counter++ < numberIterations && keepRunning) {
                for (PublicTradedCompany company : publicTradedCompanyList) {
                    String value = convertToJson(new StockTickerData(company.getPrice(), company.getSymbol()));

                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(STOCK_TICKER_TABLE_TOPIC, company.getSymbol(), value);
                    producer.send(record, callback);

                    record = new ProducerRecord<>(STOCK_TICKER_STREAM_TOPIC, company.getSymbol(), value);
                    producer.send(record, callback);

                    company.updateStockPrice();
                }
                LOG.info("Stock updates sent");
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            //LOG.info("Done generating StockTickerData Data");

        };
        executorService.submit(generateTask);
    }
    public static void produceStockTransactions(int numberIterations) {
        produceStockTransactions(numberIterations,
                DataGenerator.NUMBER_TRADED_COMPANIES,
                DataGenerator.NUMBER_UNIQUE_CUSTOMERS,
                false);
    }

    public static void produceStockTransactions(int numberIterations, int numberTradedCompanies, int numberCustomers, boolean populateGlobalTables) {
        List<PublicTradedCompany> companies = getPublicTradedCompanies(numberTradedCompanies);
        List<Customer> customers = getCustomers(numberCustomers);

        if (populateGlobalTables) {
            populateCompaniesGlobalKTable(companies);
            populateCustomersGlobalKTable(customers);
        }

        publishFinancialNews(companies);
        Runnable produceStockTransactionsTask = () -> {
            init();
            int counter = 0;
            while (counter++ < numberIterations && keepRunning) {
                List<StockTransaction> transactions = DataGenerator.generateStockTransactions(customers, companies, 50);
                List<String> jsonTransactions = convertToJson(transactions);
                for (String jsonTransaction : jsonTransactions) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(org.example.base.stock.Topics.STOCK_TRANSACTIONS.topicName(), null, jsonTransaction);
                    producer.send(record, callback);
                }
                LOG.info("Stock Transactions Batch Sent");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating stock data");

        };
        executorService.submit(produceStockTransactionsTask);
    }

    private static void publishFinancialNews(List<PublicTradedCompany> companies) {
        init();
        Set<String> industrySet = new HashSet<>();
        for (PublicTradedCompany company : companies) {
            industrySet.add(company.getIndustry());
        }
        List<String> news = DataGenerator.generateFinancialNews();
        int counter = 0;
        for (String industry : industrySet) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(FINANCIAL_NEWS.topicName(), industry, news.get(counter++));
            producer.send(record, callback);
        }
        LOG.info("Financial news sent");
    }

    private static void populateCustomersGlobalKTable(List<Customer> customers) {
        init();
        for (Customer customer : customers) {
            String customerName = customer.getLastName() + ", " + customer.getFirstName();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(CLIENTS.topicName(), customer.getCustomerId(), customerName);
            producer.send(record, callback);
        }
    }
    private static void populateCompaniesGlobalKTable(List<PublicTradedCompany> companies) {
        init();
        for (PublicTradedCompany company : companies) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(org.example.base.stock.Topics.COMPANIES.topicName(), company.getSymbol(), company.getName());
            producer.send(record, callback);
        }
    }
    private static List<Customer> getCustomers(int numberCustomers) {
        return DataGenerator.generateCustomers(numberCustomers);
    }

    private static List<PublicTradedCompany> getPublicTradedCompanies(int numberTradedCompanies) {
        return DataGenerator.generatePublicTradedCompanies(numberTradedCompanies);
    }

    private static <T> List<String> convertToJson(List<T> generatedDataItems) {
        List<String> jsonList = new ArrayList<>();
        for (T generatedData : generatedDataItems) {
            jsonList.add(convertToJson(generatedData));
        }
        return jsonList;
    }

    private static <T> String convertToJson(T generatedDataItem) {
        return gson.toJson(generatedDataItem);
    }

    public static void shutdown() {
        LOG.info("Shutting down data generation");
        keepRunning = false;

        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }

    }

    public static void produceBeerPurchases(int numberIterations) {
        Runnable produceBeerSales = () -> {
            init();
            int counter = 0;
            while (counter++ < numberIterations && keepRunning) {
                List<BeerPurchase> beerPurchases = DataGenerator.generateBeerPurchases( 50);
                List<String> jsonTransactions = convertToJson(beerPurchases);
                for (String jsonTransaction : jsonTransactions) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        Topics.BEER_PURCHASE.topicName(), null, jsonTransaction);
                    producer.send(record, callback);
                }
                LOG.info("Beer Purchases Sent");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating beer purchases");

        };
        executorService.submit(produceBeerSales);
    }
}
