package br.com.cristovamreis.batch.csv.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountOperationIn {
    String accountNumber;
    String date;
    String operation;
    String value;
    String channel;
    String success;
}