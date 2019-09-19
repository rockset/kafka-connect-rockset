# Rockset Kafka Connect Changelog

## v1.1.0 2019-09-19
- use new Rockset API Receiver Endpoint to send documents
- Retry on different 5xx errors and Socket Timeout
- Limit batch size to 1000 documents
- use blocking executor to prevent overloading the thread pool

## v1.0.1 2019-08-30
- Handle failures and report it to Kafka for retries

## v1.0.0 2019-07-23

- Initial Kafka Connect for Rockset release
- Support for Avro format
- Better logging and error messages
- Documentation of options
- Graceful retries, exactly once delivery
- Unit tests and performance testing 
