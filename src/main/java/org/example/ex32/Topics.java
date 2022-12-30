package org.example.ex32;

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
  MART_PURCHASES {
    @Override
    public String toString() {
      return "mart-purchases";
    }
  };

  public String topicName() {
    return this.toString();
  }
}
