# DKVS

* Distributed Key-Value Store
* Data is kept only on memory
* Go Client
* Communication over Raw TCP Sockets

## How TCP Packet Looks Like?

```
8       Byte -- (PL) Packet Len
1       Byte -- Msg Type for e.g.(PutOP,GetOP,JoinOP)
36      Byte -- Packet Correlation ID (UUID)
PL - 45 Byte -- Message Body
```

## Ports
* 6050 port for client communication
* 6060 port for members communication

## TODO
- [x] Use UUID of node in Partition Table DTO
- [x] Use a config yaml file for configuring nodes
- [x] Reassign partition table on node participation
- [ ] Reassign partition table on node disconnection
- [ ] Use a log library
- [ ] Implement a heartbeat mechanism
- [ ] Migrate partitions on node join 
- [ ] Add backup support