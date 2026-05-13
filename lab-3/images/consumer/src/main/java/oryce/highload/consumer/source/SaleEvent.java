package oryce.highload.consumer.source;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDate;

public record SaleEvent(
    long id,

    @JsonProperty("customer_first_name")
    String customerFirstName,
    @JsonProperty("customer_last_name")
    String customerLastName,
    @JsonProperty("customer_age")
    int customerAge,
    @JsonProperty("customer_email")
    String customerEmail,
    @JsonProperty("customer_country")
    String customerCountry,
    @JsonProperty("customer_postal_code")
    String customerPostalCode,
    @JsonProperty("customer_pet_type")
    String customerPetType,
    @JsonProperty("customer_pet_name")
    String customerPetName,
    @JsonProperty("customer_pet_breed")
    String customerPetBreed,

    @JsonProperty("seller_first_name")
    String sellerFirstName,
    @JsonProperty("seller_last_name")
    String sellerLastName,
    @JsonProperty("seller_email")
    String sellerEmail,
    @JsonProperty("seller_country")
    String sellerCountry,
    @JsonProperty("seller_postal_code")
    String sellerPostalCode,

    @JsonProperty("product_name")
    String productName,
    @JsonProperty("product_category")
    String productCategory,
    @JsonProperty("product_price")
    BigDecimal productPrice,
    @JsonProperty("product_quantity")
    int productQuantity,

    @JsonProperty("sale_date")
    @JsonFormat(pattern = "M/d/yyyy")
    LocalDate saleDate,
    @JsonProperty("sale_customer_id")
    int saleCustomerId,
    @JsonProperty("sale_seller_id")
    int saleSellerId,
    @JsonProperty("sale_product_id")
    int saleProductId,
    @JsonProperty("sale_quantity")
    int saleQuantity,
    @JsonProperty("sale_total_price")
    BigDecimal saleTotalPrice,

    @JsonProperty("store_name")
    String storeName,
    @JsonProperty("store_location")
    String storeLocation,
    @JsonProperty("store_city")
    String storeCity,
    @JsonProperty("store_state")
    String storeState,
    @JsonProperty("store_country")
    String storeCountry,
    @JsonProperty("store_phone")
    String storePhone,
    @JsonProperty("store_email")
    String storeEmail,

    @JsonProperty("pet_category")
    String petCategory,

    @JsonProperty("product_weight")
    BigDecimal productWeight,
    @JsonProperty("product_color")
    String productColor,
    @JsonProperty("product_size")
    String productSize,
    @JsonProperty("product_brand")
    String productBrand,
    @JsonProperty("product_material")
    String productMaterial,
    @JsonProperty("product_description")
    String productDescription,
    @JsonProperty("product_rating")
    BigDecimal productRating,
    @JsonProperty("product_reviews")
    int productReviews,
    @JsonProperty("product_release_date")
    @JsonFormat(pattern = "M/d/yyyy")
    LocalDate productReleaseDate,
    @JsonProperty("product_expiry_date")
    @JsonFormat(pattern = "M/d/yyyy")
    LocalDate productExpiryDate,

    @JsonProperty("supplier_name")
    String supplierName,
    @JsonProperty("supplier_contact")
    String supplierContact,
    @JsonProperty("supplier_email")
    String supplierEmail,
    @JsonProperty("supplier_phone")
    String supplierPhone,
    @JsonProperty("supplier_address")
    String supplierAddress,
    @JsonProperty("supplier_city")
    String supplierCity,
    @JsonProperty("supplier_country")
    String supplierCountry
) {
}
