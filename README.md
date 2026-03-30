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

First verify that the cluster and topic are reachable.

If Kafka is running on the Docker host:

- On macOS and Windows, use `host.docker.internal:9092` instead of `localhost:9092`.
- On Linux, either use the host IP address directly, or run the container with `--network host` and keep `localhost:9092`.

Example with `docker run`:

```bash
docker run --rm zikeyang/kafka-tests:latest \
  --bootstrap-servers host.docker.internal:9092 \
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

This setup is closer to a real workload with frequent reconnects, idle recovery, and rebalance churn.

### Step 1. Build the image

```bash
docker buildx build --platform linux/amd64 -t zikeyang/kafka-tests:latest --load .
```

### Step 2. Choose the correct Kafka address

- If Kafka is exposed from another machine or container network, pass its reachable broker address directly.
- If Kafka is on your local machine, do not use `localhost` inside the container unless you are on Linux with `--network host`.
- On macOS and Windows, replace `localhost:9092` with `host.docker.internal:9092`.

### Step 3. Run the stress test in a container

Recommended command for macOS or Windows host Docker:

```bash
docker run --rm zikeyang/kafka-tests:latest \
  --bootstrap-servers host.docker.internal:9092 \
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

If you are on Linux and want the container to share the host network namespace, you can run:

```bash
docker run --rm --network host zikeyang/kafka-tests:latest \
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

### Step 4. Run in background if needed

If you want the container to keep running in the background and inspect logs later:

```bash
docker run -d --name ksn-disconnect \
  zikeyang/kafka-tests:latest \
  --bootstrap-servers host.docker.internal:9092 \
  --topic test.disconnect \
  --group-id test.disconnect.group \
  --scenario mixed
```

Then inspect logs:

```bash
docker logs -f ksn-disconnect
```

Stop and remove the container:

```bash
docker rm -f ksn-disconnect
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

## 9. Build an AMD64 Docker Image

The repository includes a multi-stage `Dockerfile` that publishes a self-contained `linux-x64` executable and copies the compiled `ksn-disconnect` binary into the final image.

Build an `amd64` image with `docker buildx`:

```bash
docker buildx build --platform linux/amd64 -t zikeyang/kafka-tests:latest --load .
```

Show help from the image:

```bash
docker run --rm zikeyang/kafka-tests:latest --help
```

Run a normal test command:

```bash
docker run --rm zikeyang/kafka-tests:latest \
  --bootstrap-servers localhost:9092 \
  --topic test.disconnect \
  --group-id test.disconnect.group \
  --scenario steady \
  --producer-count 1 \
  --consumer-count 1 \
  --produce-rate 2
```

If you need to provide a CA file or other local files, mount them into the container:

```bash
docker run --rm \
  -v "$(pwd)/certs:/certs:ro" \
  zikeyang/kafka-tests:latest \
  --bootstrap-servers broker1:9093,broker2:9093 \
  --topic your.topic \
  --group-id your.group \
  --security-protocol SaslSsl \
  --sasl-mechanism ScramSha512 \
  --sasl-username your-user \
  --sasl-password your-password \
  --ssl-ca-location /certs/ca.pem
```

## 10. How to Read the Output

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

## 11. Tuning Suggestions

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

## 12. Notes

- The tool does not create the topic automatically. Prepare the topic in advance.
- `--partitions-hint` is only a hint for expected parallelism and does not create partitions.
- If you provide your real cluster parameters later, a recommended command can be tailored for your environment directly.
