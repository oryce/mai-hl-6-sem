package oryce.highload.consumer.sink.entity;

public record Store(
    Integer id,
    String name,
    String location,
    String city,
    String state,
    String country,
    String email,
    String phone
) {

    public Store withId(Integer id) {
        return new Store(
            id,
            name,
            location,
            city,
            state,
            country,
            email,
            phone
        );
    }
}
