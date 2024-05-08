package br.com.cristovamreis.batch.csv.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record AccountOperationModel(
    Long id,
    String accountNumber,
    LocalDate date,
    String operation,
    BigDecimal value,
    String channel,
    String success) {

    public AccountOperationModel withValue(BigDecimal value) {
        return new AccountOperationModel(
            this.id(), this.accountNumber(), this.date(), this.operation(),
            value, this.channel(), this.success()
        );
    }

    public AccountOperationModel withDate(String value) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate localDate = LocalDate.parse(value, formatter);
        return new AccountOperationModel(
                this.id(), this.accountNumber(), localDate, this.operation(),
                this.value(), this.channel(), this.success()
        );
    }
}