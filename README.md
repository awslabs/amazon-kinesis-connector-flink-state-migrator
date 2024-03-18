## Amazon Kinesis Connector State Migrator for Apache Flink

This library helps you upgrade your Apache Flink applications, without dropping state, 
from the Amazon Kinesis Connector (i.e. the package `software.amazon.kinesis.connectors`) to 
the Apache Flink Kinesis connector (`org.apache.flink.streaming.connectors.kinesis`). 
This will be necessary, for instance, if you are looking to upgrade your Flink Runtime from Flink 1.8/1.11 to Flink 1.13+.

Currently we only support the DataStream API.

**What is the Amazon Kinesis Connector?**

The old Amazon Kinesis Connector ([see on Github](https://github.com/awslabs/amazon-kinesis-connector-flink)) was 
developed to support Enhanced Fan Out (EFO) for Apache Flink 1.8/1.11. It has the Maven coordinates
`<groupId>software.amazon.kinesis</groupId> <artifactId>amazon-kinesis-connector-flink</artifactId>` and has the packaging
`software.amazon.kinesis.connectors.*`.

**What is the new Kinesis Connector?**

Also known as the Flink Kinesis Connector ([see on GitHub](https://github.com/apache/flink-connector-aws)) is the new 
Kinesis Connector that has a different package name than the old Kinesis connector: `org.apache.flink.streaming.connectors.kinesis.*`.

**What is the problem this project solves?**

The difference in the package names of the old Kinesis connector and the new Kinesis connector causes problems when
you're upgrading your Flink runtime from 1.8/1.11 to 1.13+ and want to run your upgraded app from the snapshot/state taken with the old runtime.

## How to use

Simply replace `FlinkKinesisConsumer` with `FlinkKinesisConsumerMigrator`

```
        env.addSource(new FlinkKinesisConsumerMigrator<>("myInputStream", new SimpleStringSchema(), inputProperties));
```


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

