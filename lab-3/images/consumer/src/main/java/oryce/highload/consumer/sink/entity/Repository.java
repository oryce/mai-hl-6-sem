package oryce.highload.consumer.sink.entity;

import java.io.IOException;

public interface Repository extends AutoCloseable {

    Customer upsertCustomer(Customer customer)
    throws IOException;

    Customer.Pet upsertCustomerPet(Customer.Pet pet)
    throws IOException;

    Customer.Pet.Breed upsertCustomerPetBreed(Customer.Pet.Breed breed)
    throws IOException;

    Customer.Pet.Type upsertCustomerPetType(Customer.Pet.Type type)
    throws IOException;

    Product upsertProduct(Product product)
    throws IOException;

    Product.Brand upsertProductBrand(Product.Brand brand)
    throws IOException;

    Product.Category upsertProductCategory(Product.Category category)
    throws IOException;

    Product.Color upsertProductColor(Product.Color color)
    throws IOException;

    Product.Material upsertProductMaterial(Product.Material material)
    throws IOException;

    Product.PetCategory upsertProductPetCategory(Product.PetCategory petCategory)
    throws IOException;

    Product.Size upsertProductSize(Product.Size size)
    throws IOException;

    Sale insertSale(Sale sale)
    throws IOException;

    Seller upsertSeller(Seller seller)
    throws IOException;

    Store upsertStore(Store store)
    throws IOException;

    Supplier upsertSupplier(Supplier supplier)
    throws IOException;

    void commit() throws IOException;

    void rollback() throws IOException;
}
