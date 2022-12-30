package org.example.ex61;

import java.text.DecimalFormat;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.example.base.beersales.model.BeerPurchase;
import org.example.base.beersales.model.Currency;

public class BeerPurchaseProcessor implements Processor<String, BeerPurchase, String, BeerPurchase> {

  private final String domesticSalesNode;
  private final String internationalSalesNode;
  private ProcessorContext context;

  public BeerPurchaseProcessor(String domesticSalesNode, String internationalSalesNode) {
    this.domesticSalesNode = domesticSalesNode;
    this.internationalSalesNode = internationalSalesNode;
  }

  @Override
  public void process(Record<String, BeerPurchase> record) {
    BeerPurchase beerPurchase = record.value();
    if (beerPurchase == null) {
      return;
    }

    Currency txCurrency = beerPurchase.getCurrency();

    if (!Currency.DOLLARS.equals(txCurrency)) {
      var builder = BeerPurchase.newBuilder(beerPurchase);
      double internationalSalesAmount = beerPurchase.getTotalSale();
      String pattern = "###.##";
      DecimalFormat decimalFormat = new DecimalFormat(pattern);
      builder.currency(Currency.DOLLARS);
      builder
          .totalSale(Double.parseDouble(
              decimalFormat.format(
                  txCurrency.convertToDollars(internationalSalesAmount))));
      BeerPurchase beerPurchaseInDollar = builder.build();
      context.forward(record.withValue(beerPurchaseInDollar), internationalSalesNode);
    } else {
      context.forward(record, domesticSalesNode);
    }
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public void close() {
    Processor.super.close();
  }
}
