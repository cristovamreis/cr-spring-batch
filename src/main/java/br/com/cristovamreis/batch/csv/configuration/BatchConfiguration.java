package br.com.cristovamreis.batch.csv.configuration;

import br.com.cristovamreis.batch.csv.model.AccountOperationModel;
import br.com.cristovamreis.batch.csv.dto.AccountOperationIn;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;

@Configuration
public class BatchConfiguration {

    private final PlatformTransactionManager transactionManager;
    private final JobRepository jobRepository;

    public BatchConfiguration(PlatformTransactionManager transactionManager, JobRepository jobRepository) {
        this.transactionManager = transactionManager;
        this.jobRepository = jobRepository;
    }

    @Bean
    Job job(Step step) {
        return new JobBuilder("job", jobRepository)
                .start(step)
                .incrementer(new RunIdIncrementer()) // To run more than once.
                .build();
    }

    @Bean
    Step step(ItemReader<AccountOperationIn> reader,
              ItemProcessor<AccountOperationIn, AccountOperationModel> processor,
              ItemWriter<AccountOperationModel> writer) {

        return new StepBuilder("step", jobRepository)
                .<AccountOperationIn, AccountOperationModel>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    FlatFileItemReader<AccountOperationIn> reader(
            LineMapper<AccountOperationIn> lineMapper) {
        return new FlatFileItemReaderBuilder<AccountOperationIn>()
                .name("reader")
                .lineMapper(lineMapper)
                .resource(new FileSystemResource("./files/mock-data.csv"))
                .targetType(AccountOperationIn.class)
                .build();
    }

    @Bean
    ItemProcessor<AccountOperationIn, AccountOperationModel> processor() {
        return item -> {
            return new AccountOperationModel(
                null, item.getAccountNumber(), null, item.getOperation(),
                null, item.getChannel(), item.getSuccess()
            )
            .withValue(new BigDecimal(item.getValue()))
            .withDate(item.getDate());
        };
    }

    @Bean
    JdbcBatchItemWriter<AccountOperationModel> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<AccountOperationModel>()
                .dataSource(dataSource)
                .sql("""
                        INSERT INTO AccountOperation (
                            accountNumber, "date", operation,
                            "value", channel, success)
                        VALUES (
                            :accountNumber, :date, :operation,
                            :value, :channel, :success)
                    """)
                .beanMapped()
                .build();
    }

    @Bean
    public DefaultLineMapper<AccountOperationIn> lineMapper(LineTokenizer tokenizer, FieldSetMapper<AccountOperationIn> fieldSetMapper) {
        var lineMapper = new DefaultLineMapper<AccountOperationIn>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;
    }

    @Bean
    public BeanWrapperFieldSetMapper<AccountOperationIn> fieldSetMapper() {
        var fieldSetMapper = new BeanWrapperFieldSetMapper<AccountOperationIn>();
        fieldSetMapper.setTargetType(AccountOperationIn.class);
        return fieldSetMapper;
    }

    @Bean
    public DelimitedLineTokenizer tokenizer() {
        var tokenizer = new DelimitedLineTokenizer();
        tokenizer.setDelimiter(",");
        tokenizer.setNames("accountNumber", "date", "operation", "value", "channel", "success");
        return tokenizer;
    }
}
