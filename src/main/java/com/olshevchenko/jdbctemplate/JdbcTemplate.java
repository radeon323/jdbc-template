package com.olshevchenko.jdbctemplate;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class JdbcTemplate {
    private static final String SETTER_PREFIX = "set";
    private static final String FIELD_NAME_TYPE = "TYPE";
    private DataSource dataSource;

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

    public synchronized <T> T queryForObject(String sql, RowMapper<T> rowMapper, Object... parameters) {
        String parametersForLogging = Arrays.toString(parameters);
        try (Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            injectParameters(preparedStatement, parameters);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    log.error("Object matching given parameters was not found: {} ", parametersForLogging);
                    throw new SQLException("Object matching given parameters was not found: {} ", parametersForLogging);
                }
                return rowMapper.mapRow(resultSet);
            }
        } catch (SQLException e) {
            log.error("Cannot execute query: {} ", sql, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Error while parameters setting to PreparedStatement: {} ", parametersForLogging, e);
            throw new RuntimeException(e);
        }
    }

    public synchronized int update(String sql, Object... parameters) {
        try (Connection connection = dataSource.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

            injectParameters(preparedStatement, parameters);

            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            log.error("Cannot execute query: {} ", sql, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Error while parameters setting to PreparedStatement: {} ", Arrays.toString(parameters), e);
            throw new RuntimeException(e);
        }
    }

    void injectParameters(PreparedStatement statement, Object... parameters) {
        for (int i = 0; i < parameters.length; i++) {
            int parameterIndex = i + 1;
            Class<?> clazz = parameters[i].getClass();
            Object parameter = checkAndGetParameterSuitableForSetter(parameters[i]);
            Class<?> parameterClass = parameter.getClass();
            String className = checkClassNameReturnSuitableForSetter(clazz);
            String setterName = SETTER_PREFIX + className;

            Class<? extends PreparedStatement> statementClass = null;
            Method method = null;
            Field field = null;
            try {
                if (isClassPrimitive(clazz)) {
                    field = clazz.getField(FIELD_NAME_TYPE);
                    parameterClass = (Class<?>) field.get(0);
                }
                statementClass = statement.getClass();
                method = statementClass.getMethod(setterName, int.class, parameterClass);
                method.setAccessible(true);
                method.invoke(statement, parameterIndex, parameter);
            } catch (NoSuchFieldException e) {
                log.error("Cannot find the field '{}' in the class: {} ", field, clazz.getSimpleName(), e);
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                log.error("Cannot find the method in the class: {} ", statementClass.getSimpleName(), e);
                throw new RuntimeException(e);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("""
                        Cannot invoke method: {} \r
                        preparedStatement: {} \r
                        parameterIndex: {} \r
                        parameter: {} \r""",
                        Objects.requireNonNull(method).getName(), statement, parameterIndex, parameter, e);
                throw new RuntimeException(e);
            }
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
