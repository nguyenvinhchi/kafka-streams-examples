package org.example.base.stock.model;

import java.time.Instant;
import java.util.Objects;

public class ClickEvent {
  private String symbol;
  private String link;
  private Instant timestamp;

  ClickEvent(String symbol, String link, Instant timestamp) {
    this.symbol = symbol;
    this.link = link;
    this.timestamp = timestamp;
  }

  public static ClickEventBuilder newBuilder() {
    return new ClickEventBuilder();
  }

  public String getSymbol() {
    return symbol;
  }

  public String getLink() {
    return link;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "ClickEvent{" +
        "symbol='" + symbol + '\'' +
        ", link='" + link + '\'' +
        ", timestamp=" + timestamp +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ClickEvent)) return false;

    ClickEvent that = (ClickEvent) o;

    if (!Objects.equals(symbol, that.symbol)) return false;
    if (!Objects.equals(link, that.link)) return false;
    return Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(symbol, link, timestamp);
  }

  public static class ClickEventBuilder {

    private String symbol;
    private String link;
    private Instant timestamp;

    public ClickEventBuilder withSymbol(String symbol) {
      this.symbol = symbol;
      return this;
    }

    public ClickEventBuilder withLink(String link) {
      this.link = link;
      return this;
    }

    public ClickEventBuilder withTimestamp(Instant timestamp) {
      this.timestamp = timestamp;
      return this;
    }

    public ClickEvent build() {
      return new ClickEvent(symbol, link, timestamp);
    }
  }
}
