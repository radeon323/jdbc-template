package com.olshevchenko.jdbctemplate.entity;

import lombok.*;

import java.time.LocalDateTime;

/**
 * @author Oleksandr Shevchenko
 */
@Getter
@Setter
@ToString
@Builder
@EqualsAndHashCode
public class Product {
    private int id;
    private String name;
    private String description;
    private double price;
    private LocalDateTime creationDate;
}

