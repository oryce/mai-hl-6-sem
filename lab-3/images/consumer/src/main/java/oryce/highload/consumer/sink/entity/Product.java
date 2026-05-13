package oryce.highload.consumer.sink.entity;

import java.math.BigDecimal;
import java.time.LocalDate;

public record Product(
    Integer id,
    String name,
    String description,

    BigDecimal price,
    int quantity,

    Supplier supplier,
    Category category,
    PetCategory petCategory,
    Brand brand,
    Color color,
    Material material,
    Size size,
    BigDecimal weight,

    BigDecimal rating,
    int reviews,

    LocalDate releaseDate,
    LocalDate expiryDate
) {

    public Product withId(Integer id) {
        return new Product(
            id,
            name,
            description,
            price,
            quantity,
            supplier,
            category,
            petCategory,
            brand,
            color,
            material,
            size,
            weight,
            rating,
            reviews,
            releaseDate,
            expiryDate
        );
    }

    public record Brand(Integer id, String name) {

        public Brand withId(Integer id) {
            return new Brand(id, name);
        }
    }

    public record Category(Integer id, String name) {

        public Category withId(Integer id) {
            return new Category(id, name);
        }
    }

    public record Color(Integer id, String name) {

        public Color withId(Integer id) {
            return new Color(id, name);
        }
    }

    public record Material(Integer id, String name) {

        public Material withId(Integer id) {
            return new Material(id, name);
        }
    }

    public record PetCategory(Integer id, String name) {

        public PetCategory withId(Integer id) {
            return new PetCategory(id, name);
        }
    }

    public record Size(Integer id, String name) {

        public Size withId(Integer id) {
            return new Size(id, name);
        }
    }
}
