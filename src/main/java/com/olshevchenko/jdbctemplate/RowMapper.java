package com.olshevchenko.jdbctemplate;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Oleksandr Shevchenko
 */
public interface RowMapper <T> {
    T mapRow(ResultSet resultSet) throws SQLException;
}
