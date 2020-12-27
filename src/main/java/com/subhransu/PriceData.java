package com.subhransu;

import java.io.Serializable;

//this class is used to store the price data
public class PriceData implements Serializable {
    private Double close;
    private Double high;
    private Double open;
    private Double low;
    private Double volume;

    public Double getClose() {
        return close;
    }

    public void setClose(Double close) {
        this.close = close;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getOpen() {
        return open;
    }

    public void setOpen(Double open) {
        this.open = open;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public Double getVolume() {
        return volume;
    }

    public void setVolume(Double volume) {
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "PriceData{" +
                "close=" + close +
                ", high=" + high +
                ", open=" + open +
                ", low=" + low +
                ", volume=" + volume +
                '}';
    }
}
