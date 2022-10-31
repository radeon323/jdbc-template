package com.olshevchenko.jdbctemplate;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * @author Oleksandr Shevchenko
 */
@Slf4j
@AllArgsConstructor
public class JdbcTemplate {
    private static final String SETTER_PREFIX = "set";
    private static final String FIELD_NAME_TYPE = "TYPE";
    private final DataSource dataSource;

    public synchronized <T> List<T> query(String sql, RowMapper<T> rowMapper) {
        List<T> list = Collections.synchronizedList(new ArrayList<>());
        try (Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery()) {
            while(resultSet.next()) {
                list.add(rowMapper.mapRow(resultSet));
            }
            return list;
        } catch (SQLException e) {
            log.error("Cannot execute query: {} ", sql, e);
            throw new RuntimeException(e);
        }
    }

    public synchronized <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... params) {
        String parameters = Arrays.toString(params);
        try (Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            injectParameters(preparedStatement, params);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    log.error("Object matching given parameters was not found: {} ", parameters);
                    throw new SQLException("Object matching given parameters was not found: {} ", parameters);
                }
                return rowMapper.mapRow(resultSet);
            }
        } catch (SQLException e) {
            log.error("Cannot execute query: {} ", sql, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Error while parameters setting to PreparedStatement: {} ", parameters, e);
            throw new RuntimeException(e);
        }
    }

    public synchronized int update(String sql, Object... params) {
        String parameters = Arrays.toString(params);
        try (Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            injectParameters(preparedStatement, params);

            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error("Cannot execute query: {} ", sql, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Error while parameters setting to PreparedStatement: {} ", parameters, e);
            throw new RuntimeException(e);
        }
    }

    void injectParameters(PreparedStatement statement, Object... params) throws Exception {
        for (int i = 0; i < params.length; i++) {
            int parameterIndex = i + 1;
            Class<?> clazz = params[i].getClass();
            Object parameter = checkAndGetParameterSuitableForSetter(params[i]);
            Class<?> parameterClass = parameter.getClass();
            String className = checkClassNameReturnSuitableForSetter(clazz);
            String setterName = SETTER_PREFIX + className;

            if (isClassPrimitive(clazz)) {
                Field field = clazz.getField(FIELD_NAME_TYPE);
                parameterClass = (Class<?>) field.get(0);
            }

            Class<? extends PreparedStatement> statementClass = statement.getClass();
            Method method = statementClass.getMethod(setterName, int.class, parameterClass);
            method.setAccessible(true);
            method.invoke(statement, parameterIndex, parameter);
        }
    }

    Object checkAndGetParameterSuitableForSetter(Object parameter) {
        Class<?> clazz = parameter.getClass();
        if (clazz.equals(LocalDateTime.class)) {
            parameter = Timestamp.valueOf((LocalDateTime) parameter);
        }
        if (clazz.equals(LocalDate.class)) {
            parameter = Date.valueOf((LocalDate) parameter);
        }
        if (clazz.equals(LocalTime.class)) {
            parameter = Time.valueOf((LocalTime) parameter);
        }
        return parameter;
    }

    String checkClassNameReturnSuitableForSetter(Class<?> clazz) {
        String className = clazz.getSimpleName();
        if (Objects.equals(className, "Integer")) {
            className = "Int";
        }
        if (Objects.equals(className, "LocalDateTime")) {
            className = "Timestamp";
        }
        if (Objects.equals(className, "LocalDate")) {
            className = "Date";
        }
        if (Objects.equals(className, "LocalTime")) {
            className = "Time";
        }
        return className;
    }

    boolean isClassPrimitive(Class<?> clazz) {
        return clazz.equals(Character.class) || clazz.equals(Byte.class)  ||
               clazz.equals(Boolean.class)   || clazz.equals(Long.class)  ||
               clazz.equals(Integer.class)   || clazz.equals(Short.class) ||
               clazz.equals(Double.class)    || clazz.equals(Float.class);
    }


}
