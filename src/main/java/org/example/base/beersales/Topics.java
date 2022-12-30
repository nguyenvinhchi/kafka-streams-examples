package org.example.base.beersales;

public enum Topics {
  BEER_PURCHASE {
    @Override
    public String toString() {
      return "beer-purchase";
    }
  },
  BEER_DOMESTIC_SALES {
    @Override
    public String toString() {
      return "beer-domestic_sales";
    }
  },
  BEER_INTERNATIONAL_SALES {
    @Override
    public String toString() {
      return "beer-international_sales";
    }
  };

  public String topicName() {
    return this.toString();
  }
}
