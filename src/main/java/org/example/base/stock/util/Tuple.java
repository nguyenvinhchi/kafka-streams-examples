package org.example.base.stock.util;

import java.util.Objects;

public class Tuple<L, R> {
  public final L _1;
  public final R _2;

  private Tuple(L left, R right) {
    this._1 = left;
    this._2 = right;
  }

  public static <L, R> Tuple<L, R> of(L left, R right) {
    return new Tuple<>(left, right);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tuple<?, ?> tuple = (Tuple<?, ?>) o;
    return Objects.equals(_1, tuple._1) && Objects.equals(_2, tuple._2);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_1, _2);
  }

  @Override
  public String toString() {
    return "Tuple{" +
        "_1=" + _1 +
        ", _2=" + _2 +
        '}';
  }
}
