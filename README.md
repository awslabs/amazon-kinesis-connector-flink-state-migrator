## Amazon Kinesis Connector State Migrator for Apache Flink

This library helps you upgrade your Apache Flink applications, without dropping state, 
from the Amazon Kinesis Connector (i.e. the package `software.amazon.kinesis.connectors`) to 
the Apache Flink Kinesis connector (`org.apache.flink.streaming.connectors.kinesis`). 
This will be necessary, for instance, if you are looking to upgrade your Flink Runtime from Flink 1.8/1.11 to Flink 1.13+.

Currently we only support the DataStream API.

**What is the Amazon Kinesis Connector?**

The Amazon Kinesis Connector ([see on Github](https://github.com/awslabs/amazon-kinesis-connector-flink)) was 
developed to support Enhanced Fan Out (EFO) for Apache Flink 1.8/1.11. It has the Maven coordinates
`<groupId>software.amazon.kinesis</groupId> <artifactId>amazon-kinesis-connector-flink</artifactId>` and has the packaging
`software.amazon.kinesis.connectors.*`.

**What is the Apache Kinesis Connector?**

The Apache Flink Kinesis Connector ([see on GitHub](https://github.com/apache/flink-connector-aws)) is supported by the Apache Flink community and the recommended Amazon Kinesis connector.

**What is the problem this project solves?**

This library allows you to migrate from the Amazon to Apache Kinesis connector while retaining state in the source operator. The operator state includes a map of Kinesis shard and sequence numbers. Without this library your job will fail to start.

## How to use

Add the library to your project. Example for Maven:

```
<dependency>
    <groupId>software.amazon.kinesis</groupId>
    <artifactId>amazon-kinesis-connector-flink-state-migrator</artifactId>
    <version>1.0.0</version>
</dependency>
```

Simply replace `FlinkKinesisConsumer` with `FlinkKinesisConsumerMigrator`

```
env.addSource(new FlinkKinesisConsumerMigrator<>("myInputStream", new SimpleStringSchema(), inputProperties)).uid("my-source");
```

**Important note:** The migrator does not work if you don't already have a uid set on your source.

After a successful migration to the Apache Kinesis connector, you can:
- Take a snapshot with your upgraded Flink application + connector
- Switch back to using the Apache `FlinkKinesisConsumer`
- Remove this library from your dependencies

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

