package oryce.highload.consumer.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import oryce.highload.consumer.sink.entity.Customer;
import oryce.highload.consumer.sink.entity.Product;
import oryce.highload.consumer.sink.entity.Repository;
import oryce.highload.consumer.sink.entity.Sale;
import oryce.highload.consumer.sink.entity.Seller;
import oryce.highload.consumer.sink.entity.Store;
import oryce.highload.consumer.sink.entity.Supplier;
import oryce.highload.consumer.source.SaleEvent;

import java.io.IOException;
import java.util.Objects;

public class RepositorySinkWriter implements SinkWriter<SaleEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositorySinkWriter.class);

    private final Repository repository;

    public RepositorySinkWriter(Repository repository) {
        this.repository = Objects.requireNonNull(repository, "repository");
    }

    @Override
    public void write(SaleEvent event, Context context) throws IOException {
        try {
            var customer = repository.upsertCustomer(new Customer(
                event.saleCustomerId(),
                event.customerFirstName(),
                event.customerLastName(),
                event.customerAge(),
                event.customerEmail(),
                event.customerCountry(),
                event.customerPostalCode()
            ));

            repository.upsertCustomerPet(new Customer.Pet(
                null,
                customer,
                event.customerPetName(),
                repository.upsertCustomerPetBreed(new Customer.Pet.Breed(null, event.customerPetBreed())),
                repository.upsertCustomerPetType(new Customer.Pet.Type(null, event.customerPetType()))
            ));

            var supplier = repository.upsertSupplier(new Supplier(
                null,
                event.supplierName(),
                event.supplierContact(),
                event.supplierEmail(),
                event.supplierPhone(),
                event.supplierAddress(),
                event.supplierCity(),
                event.supplierCountry()
            ));

            var product = repository.upsertProduct(new Product(
                event.saleProductId(),
                event.productName(),
                event.productDescription(),
                event.productPrice(),
                event.productQuantity(),
                supplier,
                repository.upsertProductCategory(new Product.Category(null, event.productCategory())),
                repository.upsertProductPetCategory(new Product.PetCategory(null, event.petCategory())),
                repository.upsertProductBrand(new Product.Brand(null, event.productBrand())),
                repository.upsertProductColor(new Product.Color(null, event.productColor())),
                repository.upsertProductMaterial(new Product.Material(null, event.productColor())),
                repository.upsertProductSize(new Product.Size(null, event.productSize())),
                event.productWeight(),
                event.productRating(),
                event.productReviews(),
                event.productReleaseDate(),
                event.productExpiryDate()
            ));

            var seller = repository.upsertSeller(new Seller(
                event.saleSellerId(),
                event.sellerFirstName(),
                event.sellerLastName(),
                event.sellerEmail(),
                event.sellerCountry(),
                event.sellerPostalCode()
            ));

            var store = repository.upsertStore(new Store(
                null,
                event.storeName(),
                event.storeLocation(),
                event.storeCity(),
                event.storeState(),
                event.storeCountry(),
                event.storeEmail(),
                event.storePhone()
            ));

            var sale = repository.insertSale(new Sale(
                null,
                customer,
                event.saleDate(),
                seller,
                store,
                product,
                event.saleQuantity(),
                event.saleTotalPrice()
            ));

            repository.commit();
            LOGGER.trace("Commited sale {}", sale.id());
        } catch (Exception e) {
            try {
                repository.rollback();
            } catch (IOException rollbackException) {
                e.addSuppressed(rollbackException);
            }
            throw new IOException(e);
        }
    }

    @Override
    public void flush(boolean endOfInput) {
    }

    @Override
    public void close() throws Exception {
        repository.close();
    }
}
