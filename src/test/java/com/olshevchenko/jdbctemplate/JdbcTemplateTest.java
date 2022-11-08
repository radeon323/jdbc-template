package com.olshevchenko.jdbctemplate;

import com.olshevchenko.jdbctemplate.entity.Product;
import com.olshevchenko.jdbctemplate.mapper.ProductRowMapper;
import lombok.SneakyThrows;
import org.apache.commons.dbcp2.BasicDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Oleksandr Shevchenko
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcTemplateTest {
    private static final String FIND_ALL_SQL = "SELECT id, name, description, price, creation_date FROM products";
    private static final String FIND_BY_ID_SQL = "SELECT id, name, description, price, creation_date FROM products WHERE id = ?";
    private static final String ADD_SQL = "INSERT INTO products (name, description, price, creation_date) VALUES (?, ?, ?, ?)";
    private static final String DELETE_BY_ID_SQL = "DELETE FROM products WHERE id = ?";
    private static final String UPDATE_BY_ID_SQL = "UPDATE products SET name = ?, description = ?, price = ? WHERE id = ?";

    private final ProductRowMapper PRODUCT_ROW_MAPPER = new ProductRowMapper();
    private final BasicDataSource dataSource = new BasicDataSource();
    private final List<Product> expectedProducts = new ArrayList<>();

    private final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

    private final Product productSamsung;
    private final Product productXiaomi;
    private final Product productApple;

    @SneakyThrows
    JdbcTemplateTest() {
        productSamsung = Product.builder()
                .id(1)
                .name("Samsung Galaxy M52")
                .description("6.7 inches, Qualcomm SM7325 Snapdragon 778G 5G")
                .price(13499.0)
                .creationDate(LocalDateTime.of(2022, 2,24, 4, 0, 0))
                .build();
        expectedProducts.add(productSamsung);
        productXiaomi = Product.builder()
                .id(2)
                .name("Xiaomi Redmi Note 9 Pro")
                .description("6.67 inches, Qualcomm SM7125 Snapdragon 720G Octa-core")
                .price(11699.0)
                .creationDate(LocalDateTime.of(2022, 2,24, 4, 0, 0))
                .build();
        expectedProducts.add(productXiaomi);
        productApple = Product.builder()
                .id(3)
                .name("Apple iPhone 14")
                .description("6.1 inches, Apple A15 Bionic")
                .price(41499.0)
                .creationDate(LocalDateTime.of(2022, 2,24, 4, 0, 0))
                .build();
        expectedProducts.add(productApple);

        dataSource.setUrl("jdbc:h2:mem:test");
        Connection connection = dataSource.getConnection();
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(
                        JdbcTemplateTest.class.getClassLoader().getResourceAsStream("db.sql"))));
        RunScript.execute(connection, bufferedReader);
    }

    @Test
    void testQueryReturnAListOfProducts() {
        List<Product> actualProducts = jdbcTemplate.query(FIND_ALL_SQL, PRODUCT_ROW_MAPPER);
        assertEquals(expectedProducts, actualProducts);
    }

    @Test
    void testQueryForObjectReturnAProduct() {
        Product actualProduct = jdbcTemplate.queryForObject(FIND_BY_ID_SQL, PRODUCT_ROW_MAPPER, 1);
        assertEquals(productSamsung, actualProduct);
    }

    @Test
    void testUpdateAddAndDeleteReturnANumberOfUpdatedRows() {
        Product productNokia = Product.builder()
                .id(4)
                .name("Nokia G11")
                .description("6.5 inches, Unisoc T606")
                .price(4499.0)
                .creationDate(LocalDateTime.of(2022, 2,24, 4, 0, 0))
                .build();
        int rowsModifiedBefore = jdbcTemplate.update(ADD_SQL, productNokia.getName(), productNokia.getDescription(), productNokia.getPrice(), productNokia.getCreationDate());
        assertEquals(1, rowsModifiedBefore);

        List<Product> actualProductsBefore = jdbcTemplate.query(FIND_ALL_SQL, PRODUCT_ROW_MAPPER);
        assertEquals(4, actualProductsBefore.size());
        assertTrue(actualProductsBefore.contains(productNokia));

        int rowsModifiedAfter = jdbcTemplate.update(DELETE_BY_ID_SQL, 4);
        assertEquals(1, rowsModifiedAfter);
        List<Product> actualProductsAfter = jdbcTemplate.query(FIND_ALL_SQL, PRODUCT_ROW_MAPPER);
        assertEquals(3, actualProductsAfter.size());
        assertFalse(actualProductsAfter.contains(productNokia));
        assertEquals(expectedProducts, actualProductsAfter);
    }

    @Test
    void testUpdateReturnANumberOfUpdatedRows() {
        int rowsModifiedBefore = jdbcTemplate.update(UPDATE_BY_ID_SQL, "Xiaomi Updated", productXiaomi.getDescription(), productXiaomi.getPrice(), productXiaomi.getId());
        assertEquals(1, rowsModifiedBefore);
        List<Product> actualProductsBefore = jdbcTemplate.query(FIND_ALL_SQL, PRODUCT_ROW_MAPPER);
        assertEquals("Xiaomi Updated", actualProductsBefore.get(1).getName());

        int rowsModifiedAfter = jdbcTemplate.update(UPDATE_BY_ID_SQL, productXiaomi.getName(), productXiaomi.getDescription(), productXiaomi.getPrice(), productXiaomi.getId());
        assertEquals(1, rowsModifiedAfter);
        List<Product> actualProductsAfter = jdbcTemplate.query(FIND_ALL_SQL, PRODUCT_ROW_MAPPER);
        assertEquals(productXiaomi.getName(), actualProductsAfter.get(1).getName());
        assertEquals(expectedProducts, actualProductsAfter);
    }

    @SneakyThrows
    @Test
    void testInjectParameters() {
        Connection connection = dataSource.getConnection();
        PreparedStatement initialStatement = connection.prepareStatement(ADD_SQL);
        PreparedStatement statementToExecute = connection.prepareStatement(ADD_SQL);

        jdbcTemplate.injectParameters(statementToExecute, 1);

        assertNotEquals(statementToExecute, initialStatement);
    }

    @SneakyThrows
    @Test
    void testInjectParametersInvocationThrowRuntimeException() {
        Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(ADD_SQL);
        assertThrows(RuntimeException.class, () -> jdbcTemplate.injectParameters(statement, new Object()));
    }


}