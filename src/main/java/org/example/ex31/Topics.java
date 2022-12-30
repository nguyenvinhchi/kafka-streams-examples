package org.example.ex31;

public enum Topics {
  WORDS {
    @Override
    public String toString() {
      return "basic-words";
    }
  },
  WORDS_TRANSFORMED {
    @Override
    public String toString() {
      return "basic-words-transformed";
    }
  };

  public String topicName() {
    return this.toString();
  }
}
