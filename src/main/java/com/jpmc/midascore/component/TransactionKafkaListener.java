package com.jpmc.midascore.component;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.foundation.Transaction;

@Component
public class TransactionKafkaListener {

    @KafkaListener(
        topics = "${general.kafka-topic}",
        groupId = "midas-core"
    )
    public void listen(Transaction transaction) {
        // Intentionally left blank
        // Integration only – processing comes later
        System.out.println("Received transaction: " + transaction.toString());
    }
}
