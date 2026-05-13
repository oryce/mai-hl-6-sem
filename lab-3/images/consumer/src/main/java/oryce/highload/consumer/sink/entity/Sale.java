package oryce.highload.consumer.sink.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

public record Sale(
    Integer id,
    Customer customer,
    LocalDate date,
    Seller seller,
    Store store,
    Product product,
    int quantity,
    BigDecimal totalPrice
) {

    public Sale withId(Integer id) {
        return new Sale(
            id,
            customer,
            date,
            seller,
            store,
            product,
            quantity,
            totalPrice
        );
    }
}
