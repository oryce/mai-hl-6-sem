package oryce.highload.consumer.sink.entity;

public record Supplier(
    Integer id,
    String name,
    String contact,
    String email,
    String phone,
    String address,
    String city,
    String country
) {

    public Supplier withId(Integer id) {
        return new Supplier(
            id,
            name,
            contact,
            email,
            phone,
            address,
            city,
            country
        );
    }
}
