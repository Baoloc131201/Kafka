package com.example.orderservice.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {
    @NotBlank
    private String customerId;
    @NotBlank
    @Email
    private String email;
    @NotBlank
    private String product;
    @Min(1)
    private int quantity;
    private BigDecimal amount;
}
