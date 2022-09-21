# Rockset Kafka Connect Changelog
## v1.4.1 2022-09-21

- Fix Avro parser handling nulls
- Fix timestamp handling

## v1.4.0 2022-04-14
- Add batch size parameter

## v1.3.0 2020-11-05
- Support list type messages

## v1.2.1 2019-11-22
- Don't throw errors from put. Only flush call handles failures.
- Do not block all threads during retries.

## v1.2.0 2019-10-04
- Connector now supports keys in messages for all types
- `_id` for documents are now assigned on the server side. This could cause collections to have duplicate docs for a message
- Key for a message is now part of the document, under `_meta.kafka.key`

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
