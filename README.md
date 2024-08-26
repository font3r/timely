centralised system for job management

TODO:

- [x] mvp for task scheduling
- [x] retry policy
- [ ] detecting jobs that won't start due to timeout, how can we detect if job is in 'staled' state if we 
  cannot pull status on demand. Should we set a maximum delay in status events?
- [ ] support for sync transport methods (valid only starting tasks, or push based statusing)
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