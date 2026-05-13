package oryce.highload.consumer.sink.entity;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class JdbcRepository implements Repository {

    private final Connection connection;

    public JdbcRepository(Connection connection) {
        this.connection = requireNonNull(connection, "connection");
    }

    private Integer upsertNameTable(String table, String name)
    throws IOException {
        String sql =
            """
            INSERT INTO %s(name)
            VALUES (?)
            ON CONFLICT (name) DO UPDATE SET
                name = EXCLUDED.name
            RETURNING id
            """.formatted(table);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getInt("id");
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Customer upsertCustomer(Customer customer)
    throws IOException {
        String sql =
            """
            INSERT INTO customers(
                id, first_name, last_name, age, email, country, postal_code
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                age = EXCLUDED.age,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, customer.id());
            statement.setString(2, customer.firstName());
            statement.setString(3, customer.lastName());
            statement.setInt(4, customer.age());
            statement.setString(5, customer.email());
            statement.setString(6, customer.country());
            statement.setString(7, customer.postalCode());

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return customer.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Customer.Pet upsertCustomerPet(Customer.Pet pet)
    throws IOException {
        String sql =
            """
            INSERT INTO customer_pets(
                customer_id, name, breed_id, type_id
            )
            VALUES (?, ?, ?, ?)
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, pet.customer().id());
            statement.setString(2, pet.name());
            statement.setInt(3, pet.breed().id());
            statement.setInt(4, pet.type().id());

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return pet.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Customer.Pet.Breed upsertCustomerPetBreed(Customer.Pet.Breed breed)
    throws IOException {
        Integer id = upsertNameTable("pet_breeds", breed.name());
        return breed.withId(id);
    }

    @Override
    public Customer.Pet.Type upsertCustomerPetType(Customer.Pet.Type type)
    throws IOException {
        Integer id = upsertNameTable("pet_types", type.name());
        return type.withId(id);
    }

    @Override
    public Product upsertProduct(Product product)
    throws IOException {
        String sql =
            """
            INSERT INTO products(
                id,
                name,
                description,
            
                price,
                quantity,
            
                supplier_id,
                category_id,
                pet_category_id,
                brand_id,
                color_id,
                material_id,
                size_id,
                weight,
            
                rating,
                reviews,
            
                release_date,
                expiry_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
            
                price = EXCLUDED.price,
                quantity = EXCLUDED.quantity,
            
                supplier_id = EXCLUDED.supplier_id,
                category_id = EXCLUDED.category_id,
                pet_category_id = EXCLUDED.pet_category_id,
                brand_id = EXCLUDED.brand_id,
                color_id = EXCLUDED.color_id,
                material_id = EXCLUDED.material_id,
                size_id = EXCLUDED.size_id,
                weight = EXCLUDED.weight,
            
                rating = EXCLUDED.rating,
                reviews = EXCLUDED.reviews,
            
                release_date = EXCLUDED.release_date,
                expiry_date = EXCLUDED.expiry_date
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, product.id());
            statement.setString(2, product.name());
            statement.setString(3, product.description());

            statement.setBigDecimal(4, product.price());
            statement.setInt(5, product.quantity());

            statement.setInt(6, product.supplier().id());
            statement.setInt(7, product.category().id());
            statement.setInt(8, product.petCategory().id());
            statement.setInt(9, product.brand().id());
            statement.setInt(10, product.color().id());
            statement.setInt(11, product.material().id());
            statement.setInt(12, product.size().id());
            statement.setBigDecimal(13, product.weight());

            statement.setBigDecimal(14, product.rating());
            statement.setInt(15, product.reviews());

            statement.setDate(16, Date.valueOf(product.releaseDate()));
            statement.setDate(17, Date.valueOf(product.expiryDate()));

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return product.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Product.Brand upsertProductBrand(Product.Brand brand)
    throws IOException {
        Integer id = upsertNameTable("product_brands", brand.name());
        return brand.withId(id);
    }

    @Override
    public Product.Category upsertProductCategory(Product.Category category)
    throws IOException {
        Integer id = upsertNameTable("product_categories", category.name());
        return category.withId(id);
    }

    @Override
    public Product.Color upsertProductColor(Product.Color color)
    throws IOException {
        Integer id = upsertNameTable("product_colors", color.name());
        return color.withId(id);
    }

    @Override
    public Product.Material upsertProductMaterial(Product.Material material)
    throws IOException {
        Integer id = upsertNameTable("product_materials", material.name());
        return material.withId(id);
    }

    @Override
    public Product.PetCategory upsertProductPetCategory(Product.PetCategory petCategory)
    throws IOException {
        Integer id = upsertNameTable("product_pet_categories", petCategory.name());
        return petCategory.withId(id);
    }

    @Override
    public Product.Size upsertProductSize(Product.Size size)
    throws IOException {
        Integer id = upsertNameTable("product_sizes", size.name());
        return size.withId(id);
    }

    @Override
    public Sale insertSale(Sale sale)
    throws IOException {
        String sql =
            """
            INSERT INTO sales(
                customer_id,
                date,
                seller_id,
                store_id,
                product_id,
                quantity,
                total_price
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, sale.customer().id());
            statement.setDate(2, Date.valueOf(sale.date()));
            statement.setInt(3, sale.seller().id());
            statement.setInt(4, sale.store().id());
            statement.setInt(5, sale.product().id());
            statement.setInt(6, sale.quantity());
            statement.setBigDecimal(7, sale.totalPrice());

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return sale.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Seller upsertSeller(Seller seller)
    throws IOException {
        String sql =
            """
            INSERT INTO sellers(
                id, first_name, last_name, email, country, postal_code
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, seller.id());
            statement.setString(2, seller.firstName());
            statement.setString(3, seller.lastName());
            statement.setString(4, seller.email());
            statement.setString(5, seller.country());
            statement.setString(6, seller.postalCode());

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return seller.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Store upsertStore(Store store)
    throws IOException {
        String sql =
            """
            INSERT INTO stores(
                name, location, city, state, country, email, phone
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (email) DO UPDATE SET
                name = EXCLUDED.name,
                location = EXCLUDED.location,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                country = EXCLUDED.country,
                phone = EXCLUDED.phone
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, store.name());
            statement.setString(2, store.location());
            statement.setString(3, store.city());
            statement.setString(4, store.state());
            statement.setString(5, store.country());
            statement.setString(6, store.email());
            statement.setString(7, store.phone());

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return store.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Supplier upsertSupplier(Supplier supplier)
    throws IOException {
        String sql =
            """
            INSERT INTO product_suppliers(
                name, contact, email, phone, address, city, country
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (email) DO UPDATE SET
                name = EXCLUDED.name,
                contact = EXCLUDED.contact,
                phone = EXCLUDED.phone,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                country = EXCLUDED.country
            RETURNING id
            """;

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, supplier.name());
            statement.setString(2, supplier.contact());
            statement.setString(3, supplier.email());
            statement.setString(4, supplier.phone());
            statement.setString(5, supplier.address());
            statement.setString(6, supplier.city());
            statement.setString(7, supplier.country());

            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return supplier.withId(resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void commit() throws IOException {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void rollback() throws IOException {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
