package org.example.base.stock.data.generators;

public class Customer {
    private String firstName;
    private String lastName;
    private String customerId;
    private String creditCardNumber;

    public Customer(String firstName, String lastName, String customerId, String creditCardNumber) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.customerId = customerId;
        this.creditCardNumber = creditCardNumber;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getCreditCardNumber() {
        return creditCardNumber;
    }
}
