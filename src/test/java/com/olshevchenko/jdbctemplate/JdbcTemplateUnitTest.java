package com.olshevchenko.jdbctemplate;

import com.olshevchenko.jdbctemplate.mapper.ProductRowMapper;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Oleksandr Shevchenko
 */
@ExtendWith(MockitoExtension.class)
class JdbcTemplateUnitTest {
    private final ProductRowMapper PRODUCT_ROW_MAPPER = new ProductRowMapper();

    @Mock
    private BasicDataSource dataSource;

    private final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

    @Test
    void testQueryThrowRuntimeExceptionWhenQueryNotValid() {
        assertThrows(RuntimeException.class, () -> jdbcTemplate.query("", PRODUCT_ROW_MAPPER));
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


}