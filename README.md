# Testing

server: `dotnet run -c Release --linger-ms 10 --max-fanout 400 --min-worker-threads 8 --stall-ms 10`

client: `dotnet run -c Release --clients 200 --payload-size 500 --broadcast-count 200 --duration 240`

More cli options for fiddling avaliable, use as you like.

## Ok but why

Demonstrates (in some capacity) the performance of bland C# and Quic (System.Net.Quic).

Take with a salt lamp in your mouth (shit and lazy code), but local testing showed the server broadcasting 1M msg/s @ ~3.5 gbps with 1.2k active connections.

<b>Do your own benchmarks!</b>

## Notes

- Client simulator behaves poorly with more than 200 clients in a single process.
- Both client and server are very unstable, a result of shit (and AI generated) code.
- +95% of CPU time is eaten by kernel (msquic send) for the server, more agressive message coalessing to reduce send calls would be how to get more bytes running around. However that can come at the cost of latency. I didn't do a terrible amount to optimize around this.
- stall-ms >5ms recommended
- Proper game server would do more than relaying, and would actually use some priority system to drop messages to congested clients.
- This is a sloppy weekend PoC take it with as much salt as you can consume.