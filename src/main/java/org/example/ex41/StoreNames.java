package org.example.ex41;

public enum StoreNames {
  REWARDS_STATE_STORE {
    @Override
    public String toString() {
      return "rewards-state-store";
    }
  };

  public String value() {
    return this.toString();
  }
}
