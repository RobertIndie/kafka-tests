# Kafka .NET Disconnect Reproduction Tool

This console application is built on top of `Confluent.Kafka`. It simulates producers, consumers, burst traffic, idle recovery, active client recreation, and consumer-group rebalances in a single process so you can observe unexpected disconnects, transport failures, timeouts, commit errors, and long stalls.

## 1. Prerequisites

Install `.NET 8 SDK` or later on the machine.

Verify it with:

```bash
dotnet --info
```

The project already references:

```xml
<PackageReference Include="Confluent.Kafka" Version="2.13.2" />
```

## 2. Restore, Build, and Run

For the first run:

```bash
dotnet restore
dotnet build
```

Show help:

```bash
dotnet run -- --help
```

## 3. Required Arguments

- `--bootstrap-servers`
- `--topic`

Common optional arguments:

- `--group-id`
- `--scenario steady|burst|recycle|rebalance|mixed`
- `--producer-count`
- `--consumer-count`
- `--produce-rate`
- `--burst-rate`
- `--burst-interval-sec`
- `--producer-recreate-interval-sec`
- `--consumer-recreate-interval-sec`
- `--idle-window-sec`
- `--commit-mode auto|manual`
- `--output-json true|false`

## 4. Authentication Configuration

### SASL/SSL example

```bash
dotnet run -- \
  --bootstrap-servers broker1:9093,broker2:9093 \
  --topic your.topic \
  --group-id your.group \
  --security-protocol SaslSsl \
  --sasl-mechanism ScramSha512 \
  --sasl-username your-user \
  --sasl-password your-password \
  --ssl-ca-location /path/to/ca.pem
```

If your cluster is plaintext, you can omit the security arguments and keep only `--bootstrap-servers` and `--topic`.

## 5. Scenario Selection

- `steady`
  Stable low-rate traffic with no active client recreation.
- `burst`
  Periodic traffic spikes on top of the steady baseline.
- `recycle`
  Periodically recreates producers and consumers.
- `rebalance`
  Focuses on consumer-group churn and rebalances.
- `mixed`
  Enables bursts, idle recovery, producer recycle, consumer recycle, and rebalances together.

It is recommended to run `steady` first as a baseline, then run `mixed` for stronger pressure.

## 6. Minimal Connectivity Check

First verify that the cluster and topic are reachable:

```bash
dotnet run -- \
  --bootstrap-servers localhost:9092 \
  --topic test.disconnect \
  --group-id test.disconnect.group \
  --scenario steady \
  --producer-count 1 \
  --consumer-count 1 \
  --produce-rate 2 \
  --message-bytes 512 \
  --run-duration-sec 300
```

Let it run for 5 to 10 minutes first and confirm there are no obvious connection errors.

## 7. Recommended Stress / Reconnect Command

This setup is closer to a real workload with frequent reconnects, idle recovery, and rebalance churn:

```bash
dotnet run -- \
  --bootstrap-servers localhost:9092 \
  --topic test.disconnect \
  --group-id test.disconnect.group \
  --scenario mixed \
  --producer-count 2 \
  --consumer-count 3 \
  --produce-rate 5 \
  --burst-rate 80 \
  --burst-interval-sec 15 \
  --message-bytes 2048 \
  --producer-recreate-interval-sec 20 \
  --consumer-recreate-interval-sec 25 \
  --idle-window-sec 30 \
  --handler-min-latency-ms 20 \
  --handler-max-latency-ms 250 \
  --commit-mode manual \
  --metadata-max-age-ms 30000 \
  --socket-timeout-ms 10000 \
  --request-timeout-ms 8000 \
  --connections-max-idle-ms 15000 \
  --run-duration-sec 900
```

## 8. Build a Standalone Executable

If you want to compile the program into a directly runnable executable instead of using `dotnet run`, use `dotnet publish`.

### macOS Apple Silicon

```bash
dotnet publish -c Release -r osx-arm64 --self-contained true /p:PublishSingleFile=true
```

Run it with:

```bash
./bin/Release/net8.0/osx-arm64/publish/ksn-disconnect --help
```

### macOS Intel

```bash
dotnet publish -c Release -r osx-x64 --self-contained true /p:PublishSingleFile=true
```

Run it with:

```bash
./bin/Release/net8.0/osx-x64/publish/ksn-disconnect --help
```

### Linux x64

```bash
dotnet publish -c Release -r linux-x64 --self-contained true /p:PublishSingleFile=true
```

Run it with:

```bash
./bin/Release/net8.0/linux-x64/publish/ksn-disconnect --help
```

### Windows x64

```bash
dotnet publish -c Release -r win-x64 --self-contained true /p:PublishSingleFile=true
```

Run it with:

```powershell
.\bin\Release\net8.0\win-x64\publish\ksn-disconnect.exe --help
```

### Example: run the published executable

```bash
./bin/Release/net8.0/osx-arm64/publish/ksn-disconnect \
  --bootstrap-servers localhost:9092 \
  --topic test.disconnect \
  --group-id test.disconnect.group \
  --scenario mixed \
  --producer-count 2 \
  --consumer-count 3 \
  --produce-rate 5
```

If you want a framework-dependent publish instead of a fully self-contained executable, use:

```bash
dotnet publish -c Release -r osx-arm64 --self-contained false
```

## 9. How to Read the Output

The program emits two types of output:

- Real-time event logs
  Including errors, client logs, statistics, partition assign/revoke, and offset commits.
- Periodic summary and final summary
  Including produced, consumed, commit, recreate, rebalance, and error-code aggregation statistics.

Meaning of `severity` in the final summary:

- `Expected`
  Only expected disconnects or retries were observed.
- `Suspicious`
  Transport failures, timeouts, or abnormal rebalances were observed and should be investigated.
- `Critical`
  A fatal error or unrecoverable state occurred.

Pay special attention to:

- `Local_Transport`
- `Local_MsgTimedOut`
- `BrokerTransportFailure`
- `Fatal`

If you need to send logs to a logging platform, enable JSON output:

```bash
dotnet run -- ... --output-json true
```

## 10. Tuning Suggestions

- To amplify producer-side issues:
  - Increase `--produce-rate`
  - Increase `--burst-rate`
  - Decrease `--request-timeout-ms`
  - Decrease `--connections-max-idle-ms`
  - Enable `--producer-recreate-interval-sec`

- To amplify consumer-side issues:
  - Increase `--consumer-count`
  - Enable `--consumer-recreate-interval-sec`
  - Increase `--handler-max-latency-ms`
  - Use `--commit-mode manual`

- To observe idle connection recovery:
  - Use `--scenario mixed`
  - Set `--idle-window-sec 30` or higher

- To observe rebalance churn:
  - Use multiple consumers
  - Use `--scenario rebalance` or `--scenario mixed`

## 11. Notes

- The tool does not create the topic automatically. Prepare the topic in advance.
- `--partitions-hint` is only a hint for expected parallelism and does not create partitions.
- If you provide your real cluster parameters later, a recommended command can be tailored for your environment directly.
