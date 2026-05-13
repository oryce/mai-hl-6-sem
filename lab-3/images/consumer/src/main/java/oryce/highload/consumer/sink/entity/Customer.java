package oryce.highload.consumer.sink.entity;

public record Customer(
    Integer id,
    String firstName,
    String lastName,
    int age,
    String email,
    String country,
    String postalCode
) {

    public Customer withId(Integer id) {
        return new Customer(
            id,
            firstName,
            lastName,
            age,
            email,
            country,
            postalCode
        );
    }

    public record Pet(
        Integer id,
        Customer customer,
        String name,
        Breed breed,
        Type type
    ) {

        public Pet withId(Integer id) {
            return new Pet(id, customer, name, breed, type);
        }

        public record Breed(Integer id, String name) {

            public Breed withId(Integer id) {
                return new Breed(id, name);
            }
        }

        public record Type(Integer id, String name) {

            public Type withId(Integer id) {
                return new Type(id, name);
            }
        }
    }
}
