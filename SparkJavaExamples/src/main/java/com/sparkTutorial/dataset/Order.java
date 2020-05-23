package com.sparkTutorial.dataset;

import java.util.List;

public class Order {
    private Integer customerId;
    private Long invoiceId;
    private String date;
    private List<String> itemsList;
    private List<Double> priceList;

    public Order() {}

    public Order(Integer customerId, Long invoiceId, String date, List<String> itemsList, List<Double> priceList) {
        this.customerId = customerId;
        this.invoiceId = invoiceId;
        this.date = date;
        this.itemsList = itemsList;
        this.priceList = priceList;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public Long getInvoiceId() {
        return invoiceId;
    }

    public void setInvoiceId(Long invoiceId) {
        this.invoiceId = invoiceId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<String> getItemsList() {
        return itemsList;
    }

    public void setItemsList(List<String> itemsList) {
        this.itemsList = itemsList;
    }

    public List<Double> getPriceList() {
        return priceList;
    }

    public void setPriceList(List<Double> priceList) {
        this.priceList = priceList;
    }

    @Override
    public String toString() {
        return "Order{" +
                "customerId=" + customerId +
                ", invoiceId=" + invoiceId +
                ", date='" + date + '\'' +
                ", itemsList=" + itemsList +
                ", priceList=" + priceList +
                '}';
    }
}
