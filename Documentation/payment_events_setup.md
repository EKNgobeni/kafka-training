# Payment Events Setup

## 1. Design Decisions

### Topic Design

| Setting              | Decision                                                | Justification                                                                                                                                                                                                                                            |
| -------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Topic name           | `digitalpayments-paymentevents-delete`                | Names the domain and event stream. The `-delete` suffix reflects the delete-only cleanup policy.                                                                                                                                                       |
| Partitions           | 120                                                     | Provides parallelism for consumers and spreads payment keys across the cluster. Records with the same `payment_id` go to the same partition. This is generous for a 13-event lab sample and should be sized against measured throughput in production. |
| Replication factor   | 3                                                       | Keeps three copies across the three Kafka brokers, allowing the cluster to tolerate one broker failure when the ISR requirement is met.                                                                                                                  |
| Retention time       | `220924800000` ms (2,557 days, approximately 7 years) | Preserves a long replay window for audit and recovery scenarios. Confirm that this duration meets the applicable data-retention policy.                                                                                                                  |
| Retention size       | `-1` (unlimited per partition)                        | Time is the only configured retention bound. Monitor disk usage and add a size bound if required by the cluster's storage budget.                                                                                                                        |
| Cleanup policy       | `delete`                                              | Keeps an append-only event history until retention expires; compaction would discard older records for a key and is not appropriate for this replay sample.                                                                                              |
| Compression          | `zstd`                                                | Reduces storage and network use for JSON-like event payloads.                                                                                                                                                                                            |
| Minimum ISR          | 2                                                       | With RF 3 and producer `acks=all`, a write requires at least two in-sync replicas, balancing availability with durability.                                                                                                                             |
| Maximum message size | 65,536 bytes                                            | Bounds individual event size and matches the configured topic.                                                                                                                                                                                           |

The event key is `payment_id`, so all events for a payment are on the same partition.

### Message Schema


| Field              | Type           | Description                                                                                |
| ------------------ | -------------- | ------------------------------------------------------------------------------------------ |
| `event_id`       | string         | Unique identifier for this event; useful for deduplication.                                |
| `event_type`     | string         | Lifecycle event name, such as `PaymentInitiated` or `PaymentCompleted`.                |
| `schema_version` | integer        | Version of the event contract.                                                             |
| `occurred_at`    | string         | Event time in ISO 8601 UTC format.                                                         |
| `payment_id`     | string         | Payment aggregate ID; also used as the Kafka record key.                                   |
| `sequence_no`    | integer        | One-based order of this event within a payment.                                            |
| `customer_id`    | string         | Customer associated with the payment.                                                      |
| `payer_account`  | string         | Payer account reference.                                                                   |
| `payee_account`  | string         | Payee account reference.                                                                   |
| `merchant_id`    | string         | Merchant associated with the payment.                                                      |
| `amount`         | integer        | Payment amount.                                                                            |
| `channel`        | string         | Initiation channel, such as `MOBILE_APP`or `WEB`.                                     |
| `device_id`      | string         | Device reference associated with the event.                                                |
| `device_is_new`  | boolean        | Whether the device is new for the customer, to assist with identifying fraudulent payments |
| `reason_code`    | string or null | Reason for a declined, failed, or invalidated payment; null otherwise.                     |

### Producer Design

- **Event types:** `PaymentInitiated`, `PaymentAuthorised`, `PaymentValidated`, `PaymentCompleted`, `PaymentNotAuthorised`, `PaymentNotCompleted`, and `PaymentInvalidated`.
- **Partition key:** `payment_id`; it keeps a payment's lifecycle on one partition so consumers can process its events in partition order.
- **Durability:** `acks=all`, combined with `min.insync.replicas=2`, waits for acknowledgement from the current ISR and rejects writes when fewer than two replicas are in sync.
- **Idempotency:** `enable.idempotence=true` with retries enabled prevents duplicate writes caused by producer retries within a producer session and preserves retry ordering. It does not deduplicate a manual replay or a new producer session; consumers should also deduplicate by `event_id` where needed.
- **Serialization:** JSON text for the value and the UTF-8 `payment_id` for the key. JSON is readable by the console tools and can be validated against the event schema. Python `repr` output is not JSON.
- **Retries and errors:** The producer retries transient errors (`retries=2147483647`) with backoff, bounded by `delivery.timeout.ms=10000`. Surface delivery failures, correct the cause, and replay with event-ID deduplication to avoid duplicates across producer restarts.
- **Batching:** `linger.ms=5`, `batch.size=131072`, and Zstandard compression trade a small amount of latency for better batching and throughput.
- **Mock event count:** 13 events across four payments: 4 for `PAY-000001`, 2 for `PAY-000002`, 4 for `PAY-000003`, and 3 for `PAY-000004`.

### Consumer Design

The checked-in commands currently use console consumers. The fraud-scoring behavior below is the intended business purpose; the current console command only reads and prints records.

| Consumer group                  | Purpose and events                                                                                                                                        | Processing logic                                                                                                                                                                                                         | Lag target                                                                                                                                                                    | Error handling                                                                                                                                                                                                                                    | Scalability                                                                                                                                                          |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `payments-fraud-scoring`      | Intended risk analysis for all payment lifecycle events, including initiation, authorization, validation, completion, decline, failure, and invalidation. | Validate the schema and sequence, evaluate risk signals such as amount, new device, channel, and event type, then publish a score or alert to the fraud-score stream. The current command does not implement this logic. | Design target: keep sustained lag below 30 seconds so risk signals remain useful to downstream controls. This target has not been measured.                                   | For a production processor, retry transient failures, route malformed or repeatedly failing records to a dead-letter topic, and commit offsets only after successful processing. The current console command has auto-commit disabled and no DLQ. | Up to 120 active consumers can be assigned, one per partition. The live group currently has one member; adding members only helps until all partitions are assigned. |
| `payments-console-inspection` | Manual inspection and verification of all payment events; no business side effects.                                                                       | Read from the beginning and print key and value to check payloads and ordering.                                                                                                                                          | Design target: complete an inspection within 5 minutes. This is an operator target, not a production SLA; lag cannot currently be measured because offsets are not committed. | Report parse/read failures and rerun after correcting the input. No automated retry/DLQ behavior is configured.                                                                                                                                   | The live group has three members, but this is a diagnostic group. Avoid scaling it as a production processing service.                                               |

The cluster also has a `payments-notification-dispatch` group, but its checked-in consumer subscribes to `digitalpayments-notifications-delete`, not this payment-events topic, so it is outside this topic's consumer design.

Both payment consumer commands use `auto.offset.reset=earliest`, disable auto-commit, allow up to 200 records per poll, use a 45-second session timeout and cooperative-sticky assignment, and set a 5-minute maximum poll interval. For reliable lag monitoring, production consumers must commit offsets after successful processing.

## 2. Topic Creation

Run from a shell with `kubectl` configured for the Kafka namespace/pod:

```bash
kubectl exec kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create --if-not-exists \
  --topic digitalpayments-paymentevents-delete \
  --partitions 120 \
  --replication-factor 3 \
  --config retention.ms=220924800000 \
  --config retention.bytes=-1 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=zstd \
  --config max.message.bytes=65536
```



All 120 partitions were reported with a leader and three in-sync replicas at verification time.

## 3. Producer Setup

The producer configuration used for the sample is:

```bash
kubectl exec -i kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments-paymentevents-delete \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property compression.type=zstd \
  --producer-property linger.ms=5 \
  --producer-property batch.size=131072 \
  --producer-property retry.backoff.ms=100 \
  --producer-property retry.backoff.max.ms=1000 \
  --producer-property request.timeout.ms=3000 \
  --producer-property delivery.timeout.ms=10000
```

Enter one `key:JSON-value` record per line. Example of a valid JSON record using the intended schema:

```text
PAY-000001:{"event_id":"PEVT-01-01","event_type":"PaymentInitiated","schema_version":1,"occurred_at":"2026-11-02T08:00:00Z","payment_id":"PAY-000001","sequence_no":1,"customer_id":"CUST-0001","payer_account":"1234","payee_account":"9876","merchant_id":"MERCH-0042","amount_minor":250000,"currency":"ZAR","channel":"MOBILE_APP","device_id":"DEV-9F3A01","device_is_new":false,"ip_address":"196.25.10.11","reason_code":null}
```


## 4. Consumer Groups

### `payments-fraud-scoring`

Configured command:

```bash
kubectl exec kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments-paymentevents-delete \
  --group payments-fraud-scoring \
  --from-beginning \
  --property print.key=true \
  --property key.separator=: \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property max.poll.records=200 \
  --consumer-property session.timeout.ms=45000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=100 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```


### `payments-console-inspection`

Configured command:

```bash
kubectl exec kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digitalpayments-paymentevents-delete \
  --group payments-console-inspection \
  --from-beginning \
  --property print.key=true \
  --property key.separator=: \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property max.poll.records=200 \
  --consumer-property session.timeout.ms=45000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=100 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

This group is for inspection only. Its live state was `Stable` with three members. Because auto-commit is disabled and the console consumer does not commit offsets, this group does not provide a usable committed-lag measurement.

## 5. Verification

Observed checks:

- Topic description: 120 partitions, replication factor 3, configured settings present, and three replicas in ISR for every partition.
- Offset summary: sum of earliest offsets was 15 and sum of latest offsets was 28, leaving 13 currently readable records across the topic.
- Bounded console read: `Processed a total of 13 messages`.
- Ordering: the read showed each payment's sequence in order, for example `PAY-000001` sequences 1-4 and `PAY-000004` sequences 1-3. Records for different payment IDs can interleave because they may be assigned to different partitions.
- Group state: `payments-console-inspection` was `Stable` with three members; `payments-fraud-scoring` was `Stable` with one member.
- Lag: no numeric lag was verified. The configured consumers have auto-commit disabled and no committed offsets were reported, so lag must not be inferred from group state alone.

The records were read with:

The consumer output included records keyed by `PAY-000001` through `PAY-000004` and ended with `Processed a total of 13 messages`.

## 6. Trade-offs & Justifications

- 120 partitions increase consumer parallelism but add broker metadata and file-handle overhead. This is more than the mock workload needs; retain it only if expected throughput and consumer concurrency justify it.
- RF 3 with `acks=all` and minimum ISR 2 favors durable writes while still allowing writes after one replica is unavailable. Writes stop when fewer than two replicas are in sync.
- Seven-year time retention supports replay/audit exercises, while unlimited size retention can consume substantial disk. Monitor disk and confirm retention against policy before production use.
- Delete cleanup preserves the event log for replay, unlike compaction, which retains only the latest value per key.
- `payment_id` partitioning enables per-payment ordering but may create hot partitions if a small number of payment IDs dominate traffic. No cross-payment order is guaranteed.
- Zstandard saves space and bandwidth at a CPU cost. Benchmark it against the actual event rate and broker resources.
- A console producer/consumer is useful for training and inspection, but production systems need schema validation, structured logging, offset management, and automated delivery/processing error handling.

## 7. Issues Encountered


1. **PaymentEvents** topic containing all payment messages,
   with each payment_id assigned its own partition. The offsets for partitions **89,
   51, and 67** start at  **0** , while the offset for partition **110**
   starts at  **2** . This is because messages were previously captured and later
   removed, but they had already consumed offsets within that partition.
2. **Not sure how to go about checking whether the consumer works or not**.
3. **Tried different approaches of identifying a key for the events/message**:  But I ended-up with a mixture of not working outcomes, in that "same" payments were sent to different partitions. I had to delete all messages and start from scratch after settling on the one approach that seemed to work.
