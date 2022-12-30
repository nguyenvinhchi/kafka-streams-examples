package org.example.base.stock.model;

import java.text.DecimalFormat;
import java.time.Instant;
import java.util.ArrayDeque;


public class StockPerformance {

    private static final int MAX_LOOK_BACK = 20;
    private Instant lastUpdateSent;
    private double currentPrice = 0.0;
    private double priceDifferential = 0.0;
    private double shareDifferential = 0.0;
    private int currentShareVolume = 0;
    private double currentAveragePrice = Double.MIN_VALUE;
    private double currentAverageVolume = Double.MIN_VALUE;
    private ArrayDeque<Double> shareVolumeLookback = new ArrayDeque<>(MAX_LOOK_BACK);
    private ArrayDeque<Double> sharePriceLookback = new ArrayDeque<>(MAX_LOOK_BACK);
    private transient DecimalFormat decimalFormat = new DecimalFormat("#.00");

    public ArrayDeque<Double> shareVolumeLookback() {
        return shareVolumeLookback;
    }

    public ArrayDeque<Double> sharePriceLookback() {
        return sharePriceLookback;
    }

    public void setLastUpdateSent(Instant lastUpdateSent) {
        this.lastUpdateSent = lastUpdateSent;
    }

    public void updatePriceStats(double currentPrice) {
        this.currentPrice = currentPrice;
        priceDifferential = calculateDifferentialFromAverage(currentPrice, currentAveragePrice);
        currentAveragePrice = calculateNewAverage(currentPrice, currentAveragePrice, sharePriceLookback);
    }

    public void updateVolumeStats(int currentShareVolume) {
        this.currentShareVolume = currentShareVolume;
        shareDifferential = calculateDifferentialFromAverage(currentShareVolume, currentAverageVolume);
        currentAverageVolume = calculateNewAverage(currentShareVolume, currentAverageVolume, shareVolumeLookback);
    }

    private double calculateDifferentialFromAverage(double value, double average) {
        return average != Double.MIN_VALUE ? ((value / average) - 1) * 100.0 : 0.0;
    }

    private double calculateNewAverage(double newValue, double currentAverage, ArrayDeque<Double> deque) {
        if (deque.size() < MAX_LOOK_BACK) {
            deque.add(newValue);

            if (deque.size() == MAX_LOOK_BACK) {
                currentAverage = deque.stream().reduce(0.0, Double::sum) / MAX_LOOK_BACK;
            }

        } else {
            double oldestValue = deque.poll();
            deque.add(newValue);
            currentAverage = (currentAverage + (newValue / MAX_LOOK_BACK)) - oldestValue / MAX_LOOK_BACK;
        }
        return currentAverage;
    }

    public double priceDifferential() {
        return priceDifferential;
    }

    public double volumeDifferential() {
        return shareDifferential;
    }

    public double getCurrentPrice() {
        return currentPrice;
    }

    public int getCurrentShareVolume() {
        return currentShareVolume;
    }

    public double getCurrentAveragePrice() {
        return currentAveragePrice;
    }

    public double getCurrentAverageVolume() {
        return currentAverageVolume;
    }

    public Instant getLastUpdateSent() {
        return lastUpdateSent;
    }

    public void setCurrentPrice(double currentPrice) {
        this.currentPrice = currentPrice;
    }

    public void setPriceDifferential(double priceDifferential) {
        this.priceDifferential = priceDifferential;
    }

    public void setShareDifferential(double shareDifferential) {
        this.shareDifferential = shareDifferential;
    }

    public void setCurrentShareVolume(int currentShareVolume) {
        this.currentShareVolume = currentShareVolume;
    }

    public void setCurrentAveragePrice(double currentAveragePrice) {
        this.currentAveragePrice = currentAveragePrice;
    }

    public void setCurrentAverageVolume(double currentAverageVolume) {
        this.currentAverageVolume = currentAverageVolume;
    }

    public void setShareVolumeLookback(ArrayDeque<Double> shareVolumeLookback) {
        this.shareVolumeLookback = shareVolumeLookback;
    }

    public void setSharePriceLookback(ArrayDeque<Double> sharePriceLookback) {
        this.sharePriceLookback = sharePriceLookback;
    }

    @Override
    public String toString() {
        return "StockPerformance{" +
                "lastUpdateSent= " + lastUpdateSent +
                ", currentPrice= " + decimalFormat.format(currentPrice) +
                ", currentAveragePrice= " + decimalFormat.format(currentAveragePrice) +
                ", percentage difference= " + decimalFormat.format(priceDifferential) +
                ", currentShareVolume= " + decimalFormat.format(currentShareVolume) +
                ", currentAverageVolume= " + decimalFormat.format(currentAverageVolume) +
                ", shareDifferential= " + decimalFormat.format(shareDifferential) +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Instant lastUpdateSent;
        private double currentPrice = 0.0;
        private double priceDifferential = 0.0;
        private double shareDifferential = 0.0;
        private int currentShareVolume = 0;
        private double currentAveragePrice = Double.MIN_VALUE;
        private double currentAverageVolume = Double.MIN_VALUE;
        private ArrayDeque<Double> shareVolumeLookback = new ArrayDeque<>(MAX_LOOK_BACK);
        private ArrayDeque<Double> sharePriceLookback = new ArrayDeque<>(MAX_LOOK_BACK);

        public Builder withLastUpdateSent(Instant lastUpdateSent) {
            this.lastUpdateSent = lastUpdateSent;
            return this;
        }

        public Builder withCurrentPrice(double currentPrice) {
            this.currentPrice = currentPrice;
            return this;
        }

        public Builder withPriceDifferential(double priceDifferential) {
            this.priceDifferential = priceDifferential;
            return this;
        }

        public Builder withShareDifferential(double shareDifferential) {
            this.shareDifferential = shareDifferential;
            return this;
        }

        public Builder withCurrentShareVolume(int currentShareVolume) {
            this.currentShareVolume = currentShareVolume;
            return this;
        }

        public Builder withCurrentAveragePrice(double currentAveragePrice) {
            this.currentAveragePrice = currentAveragePrice;
            return this;
        }

        public Builder withCurrentAverageVolume(double currentAverageVolume) {
            this.currentAverageVolume = currentAverageVolume;
            return this;
        }

        public Builder withShareVolumeLookback(ArrayDeque<Double> shareVolumeLookback) {
            this.shareVolumeLookback = shareVolumeLookback;
            return this;
        }

        public Builder withSharePriceLookback(ArrayDeque<Double> sharePriceLookback) {
            this.sharePriceLookback = sharePriceLookback;
            return this;
        }

        public StockPerformance build() {
            StockPerformance instance = new StockPerformance();
            instance.setLastUpdateSent(this.lastUpdateSent);
            instance.setCurrentPrice(this.currentPrice);
            instance.setCurrentAveragePrice(this.currentAveragePrice);
            instance.setCurrentAverageVolume(this.currentAverageVolume);
            instance.setPriceDifferential(this.priceDifferential);
            instance.setShareDifferential(this.shareDifferential);
            instance.setCurrentShareVolume(this.currentShareVolume);
            instance.setShareVolumeLookback(this.shareVolumeLookback);
            instance.setSharePriceLookback(this.sharePriceLookback);
            return instance;
        }
    }
}
