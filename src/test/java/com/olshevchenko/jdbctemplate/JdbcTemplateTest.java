package com.olshevchenko.jdbctemplate;

import com.olshevchenko.jdbctemplate.entity.Product;
import com.olshevchenko.jdbctemplate.mapper.ProductRowMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Oleksandr Shevchenko
 */
@Slf4j
@ExtendWith(MockitoExtension.class)
class JdbcTemplateTest {
    private static final String FIND_ALL_SQL = "SELECT id, name, description, price, creation_date FROM products";
    private static final String FIND_BY_ID_SQL = "SELECT id, name, description, price, creation_date FROM products WHERE id = ?";
    private static final String ADD_SQL = "INSERT INTO products (name, description, price, creation_date) VALUES (?, ?, ?, ?)";
    private static final String DELETE_BY_ID_SQL = "DELETE FROM products WHERE id = ?";
    private static final String UPDATE_BY_ID_SQL = "UPDATE products SET name = ?, description = ?, price = ? WHERE id = ?";

    private static final ProductRowMapper PRODUCT_ROW_MAPPER = new ProductRowMapper();
    private static final BasicDataSource dataSource = new BasicDataSource();
    private static final List<Product> expectedProducts = new ArrayList<>();

    private final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

    private static Product productSamsung;
    private static Product productXiaomi;
    private static Product productApple;

    @BeforeAll
    static void init() throws SQLException {
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
    void testQueryThrowRuntimeException() {
        Assertions.assertThrows(RuntimeException.class, () -> jdbcTemplate.query("", PRODUCT_ROW_MAPPER));
    }

    @Test
    void testQueryForObjectReturnAProduct() {
        Product actualProduct = jdbcTemplate.queryForObject(FIND_BY_ID_SQL, PRODUCT_ROW_MAPPER, 1);
        assertEquals(productSamsung, actualProduct);
    }

    @Test
    void testQueryForObjectThrowRuntimeException() {
        Assertions.assertThrows(RuntimeException.class, () -> jdbcTemplate.queryForObject("", PRODUCT_ROW_MAPPER, 1));
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

    @Test
    void testUpdateThrowRuntimeException() {
        assertThrows(RuntimeException.class, () -> jdbcTemplate.update("", "name"));
    }

    @Test
    void testCheckClassNameReturnSuitableForSetterReturnClassNameInt() {
        Class<?> clazz = Integer.class;
        String expectedClassName = "Int";
        String actualClassName = jdbcTemplate.checkClassNameReturnSuitableForSetter(clazz);
        assertEquals(expectedClassName, actualClassName);
    }

    @Test
    void testCheckClassNameReturnSuitableForSetterReturnClassNameTimestamp() {
        Class<?> clazz = LocalDateTime.class;
        String expectedClassName = "Timestamp";
        String actualClassName = jdbcTemplate.checkClassNameReturnSuitableForSetter(clazz);
        assertEquals(expectedClassName, actualClassName);
    }

    @Test
    void testCheckClassNameReturnSuitableForSetterReturnClassNameDate() {
        Class<?> clazz = LocalDate.class;
        String expectedClassName = "Date";
        String actualClassName = jdbcTemplate.checkClassNameReturnSuitableForSetter(clazz);
        assertEquals(expectedClassName, actualClassName);
    }

    @Test
    void testCheckClassNameReturnSuitableForSetterReturnClassNameTime() {
        Class<?> clazz = LocalTime.class;
        String expectedClassName = "Time";
        String actualClassName = jdbcTemplate.checkClassNameReturnSuitableForSetter(clazz);
        assertEquals(expectedClassName, actualClassName);
    }

    @Test
    void testIsClassPrimitiveReturnBoolean() {
        List<Class<?>> list = List.of(Boolean.class, Character.class, Byte.class,
                Short.class, Integer.class, Long.class, Float.class, Double.class);
        list.forEach((clazz) -> assertTrue(jdbcTemplate.isClassPrimitive(clazz)));
    }

    @Test
    void testCheckAndGetParameterSuitableForSetterReturnTimestamp() {
        Object parameter = LocalDateTime.now();
        Object actual = jdbcTemplate.checkAndGetParameterSuitableForSetter(parameter);
        assertTrue(actual instanceof Timestamp);
    }

    @Test
    void testCheckAndGetParameterSuitableForSetterReturnDate() {
        Object parameter = LocalDate.now();
        Object actual = jdbcTemplate.checkAndGetParameterSuitableForSetter(parameter);
        assertTrue(actual instanceof Date);
    }

    @Test
    void testCheckAndGetParameterSuitableForSetterReturnTime() {
        Object parameter = LocalTime.now();
        Object actual = jdbcTemplate.checkAndGetParameterSuitableForSetter(parameter);
        assertTrue(actual instanceof Time);
    }

    @Test
    void testInjectParameters() throws Exception {
        Connection connection = dataSource.getConnection();

        PreparedStatement initialStatement = connection.prepareStatement(ADD_SQL);
        PreparedStatement statementToExecute = connection.prepareStatement(ADD_SQL);

        jdbcTemplate.injectParameters(statementToExecute, 1);

        assertNotEquals(statementToExecute, initialStatement);
    }


}