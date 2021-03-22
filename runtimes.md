# Measured runtimes for jobs with various input types, without Adaptive Query Execution

## Task #1.1, job `show()`
- **17971 ms**, Parquet input, 
  where `events` are partitioned by `eventType` and `purchases` are partitioned by `isConfirmed`
- **22216 ms**, plain CSV input
- **18473 ms**, Parquet input, no partitioning

## Task #1.2, job `show()`
- **24624 ms**, Parquet input, 
  where `events` are partitioned by `eventType` and `purchases` are partitioned by `isConfirmed`
- **27063 ms**, plain CSV input
- **24400 ms**, Parquet input, no partitioning
- **28244 ms**, Parquet input, 
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`
  
## Task #2.1, job `show()`
- **22196 ms**, Parquet input,
  where `events` are partitioned by `eventType` and `purchases` are partitioned by `isConfirmed`
- **24638 ms**, plain CSV input
- **22974 ms**, Parquet input, no partitioning
- **25234 ms**, Parquet input,
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`
- **7628 ms**, input as Parquet files written after task 1.1, no partitioning there

## Task #2.2, job `show()`
- **9643 ms**, Parquet input,
  where `events` are partitioned by `eventType` and `purchases` are partitioned by `isConfirmed`
- **12889 ms**, plain CSV input
- **11195 ms**, Parquet input, no partitioning
- **15121 ms**, Parquet input,
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`
  
## Task 3.2

### September

#### Campaigns with the highest revenue, job `show()`
- **21914 ms**, input as Parquet files written after task 1.1, partitioned by date in `purchaseTime`
- **31233 ms**, plain CSV input
- **26326 ms**, Parquet input,
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`
  
#### Most popular channels in each campaign, job `show()`
- **10830 ms**, Parquet input,
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`
- **10807 ms**, plain CSV input

### 2020-11-11

#### Campaigns with the highest revenue, job `show()`
- **9318 ms**, input as Parquet files written after task 1.1, partitioned by date in `purchaseTime`
- **27487 ms**, plain CSV input
- **12731 ms**, Parquet input,
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`

#### Most popular channels in each campaign, job `show()`
- **8396 ms**, Parquet input,
  where `events` are partitioned by date in `eventTime`,
  and `purchases` are partitioned by date in `purchaseTime`
- **9235 ms**, plain CSV input
