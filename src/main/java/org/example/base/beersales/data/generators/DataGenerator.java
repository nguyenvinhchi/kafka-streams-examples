package org.example.base.beersales.data.generators;

import com.github.javafaker.Faker;
import com.github.javafaker.Finance;
import com.github.javafaker.Name;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.example.base.beersales.model.BeerPurchase;
import org.example.base.beersales.model.Currency;
import org.example.base.stock.data.generators.Customer;
import org.example.base.stock.model.PublicTradedCompany;
import org.example.base.stock.model.StockTransaction;

public class DataGenerator {

    public static final int NUMBER_UNIQUE_CUSTOMERS = 100;
    public static final int NUMBER_TRADED_COMPANIES = 50;

    private static Faker dateFaker = new Faker();
    private static Supplier<Date> timestampGenerator =
            () -> dateFaker.date().past(15, TimeUnit.MINUTES, new Date());

    private DataGenerator() {
    }

    private static List<String> generateCreditCardNumbers(int numberCards) {
        int counter = 0;
        Pattern visaMasterCardAmex = Pattern.compile("(\\d{4}-){3}\\d{4}");
        List<String> creditCardNumbers = new ArrayList<>(numberCards);
        Finance finance = new Faker().finance();
        while (counter < numberCards) {
            String cardNumber = finance.creditCard();
            if (visaMasterCardAmex.matcher(cardNumber).matches()) {
                creditCardNumbers.add(cardNumber);
                counter++;
            }
        }
        return creditCardNumbers;
    }

    public static List<Customer> generateCustomers(int numberCustomers) {
        List<Customer> customers = new ArrayList<>(numberCustomers);
        Faker faker = new Faker();
        List<String> creditCards = generateCreditCardNumbers(numberCustomers);
        for (int i = 0; i < numberCustomers; i++) {
            Name name = faker.name();
            String creditCard = creditCards.get(i);
            String customerId = faker.idNumber().valid();
            customers.add(new Customer(name.firstName(), name.lastName(), customerId, creditCard));
        }
        return customers;
    }

    public static StockTransaction generateStockTransaction() {
        List<Customer> customers = generateCustomers(1);
        List<PublicTradedCompany> companies = generatePublicTradedCompanies(1);
        return generateStockTransactions(customers, companies, 1).get(0);
    }

    public static List<StockTransaction> generateStockTransactions(List<Customer> customers, List<PublicTradedCompany> companies, int number) {
        List<StockTransaction> transactions = new ArrayList<>(number);
        Faker faker = new Faker();
        for (int i = 0; i < number; i++) {
            int numberShares = faker.number().numberBetween(100, 50000);
            Customer customer = customers.get(faker.number().numberBetween(0, customers.size()));
            PublicTradedCompany company = companies.get(faker.number().numberBetween(0, companies.size()));
            Date transactionDate = timestampGenerator.get();
            StockTransaction transaction = StockTransaction.newBuilder().withCustomerId(customer.getCustomerId()).withTransactionTimestamp(transactionDate)
                    .withIndustry(company.getIndustry()).withSector(company.getSector()).withSharePrice(company.updateStockPrice()).withShares(numberShares)
                    .withSymbol(company.getSymbol()).withPurchase(true).build();
            transactions.add(transaction);
        }
        return transactions;
    }

    public static List<StockTransaction> generateStockTransactionsForIQ(int number) {
        return generateStockTransactions(generateCustomersForInteractiveQueries(),
                generatePublicTradedCompaniesForInteractiveQueries(), number);
    }

    public static List<PublicTradedCompany> stockTicker(int numberCompanies) {
        return generatePublicTradedCompanies(numberCompanies);
    }
    public static List<PublicTradedCompany> generatePublicTradedCompanies(int numberCompanies) {
        List<PublicTradedCompany> companies = new ArrayList<>();
        Faker faker = new Faker();
        Random random = new Random();
        for (int i = 0; i < numberCompanies; i++) {
            String name = faker.company().name();
            String stripped = name.replaceAll("[^A-Za-z]", "");
            int start = random.nextInt(stripped.length() - 4);
            String symbol = stripped.substring(start, start + 4);
            double volatility = Double.parseDouble(faker.options().option("0.01", "0.02", "0.03", "0.04", "0.05", "0.06", "0.07", "0.08", "0.09"));
            double lastSold = faker.number().randomDouble(2, 15, 150);
            String sector = faker.options().option("Energy", "Finance", "Technology", "Transportation", "Health Care");
            String industry = faker.options().option("Oil & Gas Production", "Coal Mining", "Commercial Banks", "Finance/Investors Services", "Computer Communications Equipment", "Software Consulting", "Aerospace", "Railroads", "Major Pharmaceuticals");
            companies.add(new PublicTradedCompany(volatility, lastSold, symbol, name, sector, industry));
        }
        return companies;
    }

    public static List<Customer> generateCustomersForInteractiveQueries() {
        List<Customer> customers = new ArrayList<>(10);
        List<String> customerIds = Arrays.asList("12345678", "222333444", "33311111", "55556666", "4488990011", "77777799", "111188886","98765432", "665552228", "660309116");
        Faker faker = new Faker();
        List<String> creditCards = generateCreditCardNumbers(10);
        for (int i = 0; i < 10; i++) {
            Name name = faker.name();
            String creditCard = creditCards.get(i);
            String customerId = customerIds.get(i);
            customers.add(new Customer(name.firstName(), name.lastName(), customerId, creditCard));
        }
        return customers;
    }

        public static List<PublicTradedCompany> generatePublicTradedCompaniesForInteractiveQueries() {
        List<String> symbols = Arrays.asList("AEBB", "VABC", "ALBC", "EABC", "BWBC", "BNBC", "MASH", "BARX", "WNBC", "WKRP");
        List<String> companyName = Arrays.asList("Acme Builders", "Vector Abbot Corp","Albatros Enterprise", "Enterprise Atlantic",
                "Bell Weather Boilers","Broadcast Networking","Mobile Surgical", "Barometer Express", "Washington National Business Corp","Cincinnati Radio Corp.");
        List<PublicTradedCompany> companies = new ArrayList<>();
        Faker faker = new Faker();

        for (int i = 0; i < symbols.size(); i++) {
            double volatility = Double.parseDouble(faker.options().option("0.01", "0.02", "0.03", "0.04", "0.05", "0.06", "0.07", "0.08", "0.09"));
            double lastSold = faker.number().randomDouble(2, 15, 150);
            String sector = faker.options().option("Energy", "Finance", "Technology", "Transportation", "Health Care");
            String industry = faker.options().option("Oil & Gas Production", "Coal Mining", "Commercial Banks", "Finance/Investors Services", "Computer Communications Equipment", "Software Consulting", "Aerospace", "Railroads", "Major Pharmaceuticals");
            companies.add(new PublicTradedCompany(volatility, lastSold, symbols.get(i), companyName.get(i), sector, industry));
        }
        return companies;
    }

    public static List<String> generateFinancialNews() {
        List<String> news = new ArrayList<>(9);
        Faker faker = new Faker();
        for (int i = 0; i < 9; i++) {
            news.add(faker.company().bs());
        }
        return news;
    }

    public static void setTimestampGenerator(Supplier<Date> timestampGenerator) {
        DataGenerator.timestampGenerator = timestampGenerator;
    }

    public static List<BeerPurchase> generateBeerPurchases(int number) {
        List<BeerPurchase> beerPurchases = new ArrayList<>(number);
        Faker faker = new Faker();
        for (int i = 0; i < number; i++) {
            Currency currency = Currency.values()[faker.number().numberBetween(0,4)];
            String beerType = faker.beer().name();
            int cases = faker.number().numberBetween(1,15);
            double totalSale = faker.number().randomDouble(3,12, 200);
            String pattern = "###.##";
            DecimalFormat decimalFormat = new DecimalFormat(pattern);
            double formattedSale = Double.parseDouble(decimalFormat.format(totalSale));
            beerPurchases.add(
                BeerPurchase.newBuilder()
                    .beerType(beerType)
                    .currency(currency)
                    .numberCases(cases)
                    .totalSale(formattedSale).build());
        }
        return beerPurchases;
    }
}
