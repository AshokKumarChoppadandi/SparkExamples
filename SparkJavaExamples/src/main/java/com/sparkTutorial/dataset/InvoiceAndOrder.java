package com.sparkTutorial.dataset;

public class InvoiceAndOrder {
    private Long invoiceId;
    private Double price;

    public InvoiceAndOrder() {}

    public InvoiceAndOrder(Long invoiceId, Double price) {
        this.invoiceId = invoiceId;
        this.price = price;
    }

    public Long getInvoiceId() {
        return invoiceId;
    }

    public void setInvoiceId(Long invoiceId) {
        this.invoiceId = invoiceId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "InvoiceAndOrder{" +
                "invoiceId=" + invoiceId +
                ", price=" + price +
                '}';
    }
}
