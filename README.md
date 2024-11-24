[![Build and test](https://github.com/font3r/timely/actions/workflows/build-and-test.yml/badge.svg?branch=main)](https://github.com/font3r/timely/actions/workflows/build-and-test.yml)

centralised system for job management

TODO:

- [x] mvp for task scheduling
- [x] retry policy
- [ ] detecting jobs that won't start due to timeout, detect if job is in 'staled' state (does not send status events)
- [x] support for sync transport interface (rest/grpc) (valid only starting tasks, or push based job status)
  - for job statusing we have to use push based model because of load balancing/consumer groups on horizontally scaled instanced
- [x] basic persistance layer
- [x] sending job with context
- [x] scheduling engine (based on cron?)
- [ ] HA support
- [ ] client sdk, api
- [ ] job run statistics
- [ ] pausing schedules
- [x] support for single use schedules with delay (similar to ASB scheduled message)
- [ ] auth
- [ ] admin panel
- [ ] data retention
- [ ] integration tests
- [ ] support for other async transports (eg. Kafka)