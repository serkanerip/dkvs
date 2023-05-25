# DKVS

* Distributed Key-Value Store
* Data is only kept on memory
* Ready to use Go Client
* Communication over Raw TCP Sockets
* Leader Election Support with ETCD

## How TCP Packet Looks Like?

```
8       Byte -- (PL) Packet Len
1       Byte -- Msg Type for e.g.(PutOP,GetOP,JoinOP)
36      Byte -- Packet Correlation ID (UUID)
PL - 45 Byte -- Message Body
```

## TODO
- [ ] Implement a heartbeat mechanism
- [ ] Finish Repartitioning Details
- [ ] Add backup support