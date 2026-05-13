package oryce.highload.consumer.sink.entity;

public record Seller(
    Integer id,
    String firstName,
    String lastName,
    String email,
    String country,
    String postalCode
) {

    public Seller withId(Integer id) {
        return new Seller(
            id,
            firstName,
            lastName,
            email,
            country,
            postalCode
        );
    }
}
