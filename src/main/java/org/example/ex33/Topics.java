package org.example.ex33;

public enum Topics {
  MART_TRANSACTIONS {
    @Override
    public String toString() {
      return "mart-transactions";
    }
  },
  MART_PATTERNS {
    @Override
    public String toString() {
      return "mart-purchase-patterns";
    }
  },
  MART_REWARDS {
    @Override
    public String toString() {
      return "mart-rewards";
    }
  },
  MART_COFFEE {
    @Override
    public String toString() {
      return "mart-coffee";
    }
  },
  MART_ELECTRONICS {
    @Override
    public String toString() {
      return "mart-electronics";
    }
  },
  MART_PURCHASES {
    @Override
    public String toString() {
      return "mart-purchases";
    }
  };

  public String value() {
    return this.toString();
  }
}
