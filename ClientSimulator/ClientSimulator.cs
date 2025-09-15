using System;
using System.Buffers;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Serilog;
using Serilog.Events;
using Humanizer;
using Humanizer.Bytes;
using System.Net.Sockets;
using System.Text;
using System.Buffers.Binary;

#region Protocol

public enum MessageType : byte
{
    Movement = 1,
    Chat = 2,
    GameEvent = 3,
    Ping = 4
}

public enum Priority : byte
{
    Critical = 0,
    High = 1,
    Normal = 2,
    Low = 3
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct StreamHeader
{
    public MessageType StreamType;
    public ushort FixedPayloadSize;
    public Priority MessagePriority;
    public const int Size = 4;

    public static ushort PayloadSize(MessageType messageType, int fallback = 200) => messageType switch
    {
        MessageType.Movement => 200,
        MessageType.Chat => 300,
        MessageType.GameEvent => 250,
        MessageType.Ping => 50,
        _ => (ushort)fallback
    };
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct MessageFrame
{
    public ushort BroadcastCount;
    public uint MessageId;
    public long TimestampTicks;
    public const int Size = 14;
}

#endregion

#region Client

public class MessageTypeConfig
{
    public MessageType Type { get; init; }
    public int PayloadSize { get; init; } = 200;
    public int RatePerSecond { get; init; } = 20;
    public bool Enabled { get; init; } = true;
}

public sealed class ManagedOutgoingClientStream : IDisposable
{
    public QuicStream Stream { get; }
    public MessageType Type { get; }
    public ushort PayloadSize { get; }
    public bool IsDisposed { get; private set; }
    readonly object _disposeLock = new();

    public ManagedOutgoingClientStream(QuicStream stream, MessageType type, ushort payloadSize)
    {
        Stream = stream;
        Type = type;
        PayloadSize = payloadSize;
    }

    public async Task<bool> TryWriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        lock (_disposeLock)
        {
            if (IsDisposed) return false;
        }
        await Stream.WriteAsync(data, cancellationToken);
        return true;
    }

    public void Dispose()
    {
        lock (_disposeLock)
        {
            if (IsDisposed) return;
            IsDisposed = true;
            try { Stream.Dispose(); } catch { }
        }
    }
}

public sealed class GameTestClient : IDisposable
{
    readonly ILogger _logger;
    readonly List<MessageTypeConfig> _messageConfigs;
    readonly CancellationTokenSource _cancellationSource;
    readonly Random _rng;
    readonly int _clientId;
    readonly int _broadcastCount;
    readonly double _jitterFrac;

    QuicConnection? _connection;

    // Outbound: one channel per message type, one writer task per type (no cross-type HOL)
    readonly Dictionary<MessageType, Channel<ArraySegment<byte>>> _outChannels = new();
    readonly Dictionary<MessageType, Task> _writerTasks = new();

    // Outbound streams: client -> server (unidirectional)
    readonly Dictionary<MessageType, ManagedOutgoingClientStream> _outgoingStreams = new();

    // Performance counters
    long _messagesSent, _bytesSent, _messagesReceived, _bytesReceived, _nextMessageId;

    public (long sent, long sentBytes, long recv, long recvBytes) ReadTotals()
        => (Interlocked.Read(ref _messagesSent),
            Interlocked.Read(ref _bytesSent),
            Interlocked.Read(ref _messagesReceived),
            Interlocked.Read(ref _bytesReceived));


    public GameTestClient(ILogger logger, List<MessageTypeConfig> messageConfigs, int clientId, int broadcastCount, double jitterFrac)
    {
        _logger = logger;
        _messageConfigs = messageConfigs;
        _clientId = clientId;
        _broadcastCount = broadcastCount;
        _jitterFrac = Math.Clamp(jitterFrac, 0, 0.9);
        _cancellationSource = new CancellationTokenSource();
        _rng = new Random(unchecked(Environment.TickCount * 397) ^ clientId);
    }

    public async Task ConnectAsync(IPEndPoint serverEndPoint)
    {
        try
        {
            var clientConnectionOptions = new QuicClientConnectionOptions
            {
                DefaultStreamErrorCode = 0x0A,
                DefaultCloseErrorCode = 0x0B,
                RemoteEndPoint = serverEndPoint,
                MaxInboundUnidirectionalStreams = 100,
                MaxInboundBidirectionalStreams = 10,
                ClientAuthenticationOptions = new SslClientAuthenticationOptions
                {
                    ApplicationProtocols = new List<SslApplicationProtocol> { new("game-relay") },
                    RemoteCertificateValidationCallback = (_, _, _, _) => true
                },
                HandshakeTimeout = TimeSpan.FromSeconds(60),
                IdleTimeout = TimeSpan.FromMinutes(5),
            };

            _connection = await QuicConnection.ConnectAsync(clientConnectionOptions, _cancellationSource.Token);
            _logger.Debug("Connected to {EndPoint}", serverEndPoint);
            await SendReadyAsync();


            await CreateOutgoingStreams();

            // Start dedicated writer tasks per type
            foreach (var cfg in _messageConfigs)
            {
                if (!cfg.Enabled || !_outgoingStreams.ContainsKey(cfg.Type)) continue;

                // Movement queue: small & DropOldest to keep freshest
                var cap = cfg.Type == MessageType.Movement ? 128 : 256;
                var ch = Channel.CreateBounded<ArraySegment<byte>>(new BoundedChannelOptions(cap)
                {
                    FullMode = cfg.Type == MessageType.Movement
                        ? BoundedChannelFullMode.DropOldest
                        : BoundedChannelFullMode.DropWrite,
                    SingleReader = true,
                    SingleWriter = true // one producer per type
                });
                _outChannels[cfg.Type] = ch;
                _writerTasks[cfg.Type] = Task.Run(() => WriterLoopAsync(cfg.Type, ch.Reader), _cancellationSource.Token);
            }

            // Start jittered send loops per type
            foreach (var cfg in _messageConfigs)
            {
                if (!cfg.Enabled || cfg.RatePerSecond <= 0) continue;
                _ = Task.Run(() => ProducerLoopAsync(cfg), _cancellationSource.Token);
            }

            // Start receiving broadcast streams
            _ = Task.Run(ReceiveBroadcastStreams, _cancellationSource.Token);

            // Perf logger
            _ = Task.Run(MonitorPerformance, _cancellationSource.Token);
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Failed to connect");
            throw;
        }
    }

    public async Task SendReadyAsync()
    {
        using var ctrl = await _connection!.OpenOutboundStreamAsync(QuicStreamType.Bidirectional);
        var buf = new byte[12]; // "RDY1" (4) + clientId(int32) + reserved(int32)
        Encoding.ASCII.GetBytes("RDY1").CopyTo(buf, 0);
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(4), _clientId);
        BinaryPrimitives.WriteInt32LittleEndian(buf.AsSpan(8), 0);

        await ctrl.WriteAsync(buf);
        await ctrl.FlushAsync();
        ctrl.CompleteWrites();

        // optional tiny ACK (read 2 bytes "OK")
        var ack = new byte[2];
        var read = await ctrl.ReadAsync(ack);
        if (read == 2 && ack[0] == (byte)'O' && ack[1] == (byte)'K')
            _logger.Information("Server ACK READY");
        else
            _logger.Warning("Server did not ACK READY cleanly (read={Read})", read);
    }

    async Task CreateOutgoingStreams()
    {
        foreach (var config in _messageConfigs)
        {
            if (!config.Enabled) continue;

            try
            {
                var stream = await _connection!.OpenOutboundStreamAsync(QuicStreamType.Unidirectional, _cancellationSource.Token);
                var priority = config.Type switch
                {
                    MessageType.Ping => Priority.Critical,
                    MessageType.Movement => Priority.High,
                    MessageType.GameEvent => Priority.Normal,
                    MessageType.Chat => Priority.Normal,
                    _ => Priority.Normal
                };

                var header = new StreamHeader
                {
                    StreamType = config.Type,
                    FixedPayloadSize = (ushort)config.PayloadSize,
                    MessagePriority = priority
                };
                var headerBytes = new byte[StreamHeader.Size];
                MemoryMarshal.Write(headerBytes, in header);
                await stream.WriteAsync(headerBytes, _cancellationSource.Token);

                _outgoingStreams[config.Type] = new ManagedOutgoingClientStream(stream, config.Type, (ushort)config.PayloadSize);
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Failed to create outbound stream for {Type}", config.Type);
            }
        }
    }

    async Task ProducerLoopAsync(MessageTypeConfig cfg)
    {
        var baseInterval = TimeSpan.FromSeconds(1.0 / cfg.RatePerSecond);
        var token = _cancellationSource.Token;

        await Task.Delay(TimeSpan.FromMilliseconds(_rng.Next(5, 50)), token);

        while (!token.IsCancellationRequested)
        {
            try
            {
                int totalSize = MessageFrame.Size + cfg.PayloadSize;
                var rented = ArrayPool<byte>.Shared.Rent(totalSize);

                var messageId = (uint)Interlocked.Increment(ref _nextMessageId);
                var frame = new MessageFrame
                {
                    BroadcastCount = (ushort)_broadcastCount,
                    MessageId = messageId,
                    TimestampTicks = DateTime.UtcNow.Ticks
                };

                MemoryMarshal.Write(rented.AsSpan(0, MessageFrame.Size), in frame);
                Random.Shared.NextBytes(rented.AsSpan(MessageFrame.Size, cfg.PayloadSize));

                if (_outChannels.TryGetValue(cfg.Type, out var ch))
                {
                    // hand off the pooled array + length (no copy)
                    if (!ch.Writer.TryWrite(new ArraySegment<byte>(rented, 0, totalSize)))
                        ArrayPool<byte>.Shared.Return(rented); // dropped
                }
                else
                {
                    ArrayPool<byte>.Shared.Return(rented);
                }
            }
            catch (Exception ex)
            {
                _logger.Debug(ex, "Producer error for {Type}", cfg.Type);
            }

            var j = 1.0 + ((_rng.NextDouble() * 2 - 1) * _jitterFrac);
            var delay = TimeSpan.FromMilliseconds(Math.Max(1, baseInterval.TotalMilliseconds * j));
            try { await Task.Delay(delay, token); } catch (OperationCanceledException) { }
        }
    }


    async Task WriterLoopAsync(MessageType type, ChannelReader<ArraySegment<byte>> reader)
    {
        var token = _cancellationSource.Token;
        try
        {
            while (await reader.WaitToReadAsync(token))
            {
                while (reader.TryRead(out var seg))
                {
                    try
                    {
                        if (!_outgoingStreams.TryGetValue(type, out var stream) || stream.IsDisposed)
                        {
                            if (seg.Array != null) ArrayPool<byte>.Shared.Return(seg.Array);
                            break;
                        }

                        // write exactly the segment
                        var ok = await stream.TryWriteAsync(seg.AsMemory(), token);
                        if (ok)
                        {
                            Interlocked.Increment(ref _messagesSent);
                            Interlocked.Add(ref _bytesSent, seg.Count);
                        }
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        _logger.Debug(ex, "Writer error for {Type}", type);
                    }
                    finally
                    {
                        if (seg.Array != null) ArrayPool<byte>.Shared.Return(seg.Array);
                    }
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    async Task ReceiveBroadcastStreams()
    {
        var token = _cancellationSource.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                var stream = await _connection!.AcceptInboundStreamAsync(token);
                _ = Task.Run(() => HandleIncomingBroadcastStream(stream), token);
            }
        }
        catch (OperationCanceledException) { }
        catch (QuicException ex) when (ex.QuicError == QuicError.ConnectionAborted)
        {
            _logger.Information("Connection aborted");
        }
        catch (Exception ex)
        {
            _logger.Error(ex, "Accept inbound broadcast streams");
        }
    }

    async Task HandleIncomingBroadcastStream(QuicStream stream)
    {
        using (stream)
        {
            var token = _cancellationSource.Token;
            try
            {
                var headerBuffer = new byte[StreamHeader.Size];
                var headerBytesRead = 0;

                while (headerBytesRead < StreamHeader.Size)
                {
                    var bytesRead = await stream.ReadAsync(headerBuffer.AsMemory(headerBytesRead), token);
                    if (bytesRead == 0 && stream.ReadsClosed.IsCompleted) return;
                    headerBytesRead += bytesRead;
                }

                var header = MemoryMarshal.Read<StreamHeader>(headerBuffer);
                var messageSize = MessageFrame.Size + header.FixedPayloadSize;
                var buffer = ArrayPool<byte>.Shared.Rent(messageSize);

                try
                {
                    while (!token.IsCancellationRequested)
                    {
                        int totalBytesRead = 0;
                        while (totalBytesRead < messageSize)
                        {
                            var memorySlice = buffer.AsMemory(totalBytesRead, messageSize - totalBytesRead);
                            int bytesRead = await stream.ReadAsync(memorySlice, token);
                            if (bytesRead == 0 && stream.ReadsClosed.IsCompleted) return;
                            totalBytesRead += bytesRead;
                        }

                        Interlocked.Increment(ref _messagesReceived);
                        Interlocked.Add(ref _bytesReceived, messageSize);
                    }
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
            }
            catch (OperationCanceledException) { }
            catch (QuicException ex) when (ex.QuicError == QuicError.StreamAborted)
            {
                _logger.Debug("Broadcast stream aborted by server");
            }
            catch (Exception ex)
            {
                _logger.Warning(ex, "Incoming broadcast stream error");
            }
        }
    }

    async Task MonitorPerformance()
    {
        var lastSent = 0L;
        var lastReceived = 0L;
        var lastBytesSent = 0L;
        var lastBytesReceived = 0L;

        var token = _cancellationSource.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(5000, token);

                var currentSent = Interlocked.Read(ref _messagesSent);
                var currentReceived = Interlocked.Read(ref _messagesReceived);
                var currentBytesSent = Interlocked.Read(ref _bytesSent);
                var currentBytesReceived = Interlocked.Read(ref _bytesReceived);

                var sentRate = (currentSent - lastSent) / 5.0;
                var receivedRate = (currentReceived - lastReceived) / 5.0;
                var sentBytesRate = (currentBytesSent - lastBytesSent) / 5.0;
                var receivedKBps = ((currentBytesReceived - lastBytesReceived) / 5.0) / 1024.0;

                _logger.Debug("Perf: Sent {SentRate:F1} msg/s ({SentBps:F0} B/s), Recv {RecvRate:F1} msg/s ({RecvKBps:F0} kB/s)",
                    sentRate, sentBytesRate, receivedRate, receivedKBps);

                lastSent = currentSent;
                lastReceived = currentReceived;
                lastBytesSent = currentBytesSent;
                lastBytesReceived = currentBytesReceived;
            }
        }
        catch (OperationCanceledException) { }
    }

    public void Dispose()
    {
        _cancellationSource.Cancel();

        foreach (var kv in _outChannels)
            kv.Value.Writer.TryComplete();

        foreach (var stream in _outgoingStreams.Values)
            stream.Dispose();

        try
        {
            _connection?.DisposeAsync().AsTask().Wait(500);
        }
        catch { }

        _cancellationSource.Dispose();
    }
}

#endregion

#region Program

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var serverOption = new Option<string>("--server", () => "127.0.0.1", "Server hostname/IP");
        var portOption = new Option<int>("--port", () => 4433, "Server port");
        var clientsOption = new Option<int>("--clients", () => 1, "Concurrent clients");
        var durationOption = new Option<int>("--duration", () => 60, "Test duration in seconds");
        var aggIntervalOption = new Option<int>("--agg-interval", () => 2, "Seconds between aggregate stats lines.");
        var rampMsOption = new Option<int>("--ramp-ms", () => 10, "Delay between client connects (ms)");

        var movementRateOption = new Option<int>("--movement-rate", () => 20, "Movement msgs/s");
        var chatRateOption = new Option<int>("--chat-rate", () => 1, "Chat msgs/s");
        var gameEventRateOption = new Option<int>("--game-event-rate", () => 5, "GameEvent msgs/s");
        var pingRateOption = new Option<int>("--ping-rate", () => 1, "Ping msgs/s");
        var payloadSizeOption = new Option<int>("--payload-size", () => 200, "Payload size for Movement");
        var broadcastCountOption = new Option<int>("--broadcast-count", () => 200, "Requested fanout per message");
        var jitterOption = new Option<double>("--rate-jitter", () => 0.2, "Fractional jitter [0..0.9]");
        var fileLogsOption = new Option<bool>("--file-logs", () => false, "Also log to disk");

        var root = new RootCommand("QUIC Game Client Load Tester")
        {
            serverOption, portOption, clientsOption, durationOption, aggIntervalOption, rampMsOption,
            movementRateOption, chatRateOption, gameEventRateOption, pingRateOption, payloadSizeOption,
            broadcastCountOption, jitterOption, fileLogsOption
        };

        root.SetHandler(async (InvocationContext ctx) =>
        {
            var server = ctx.ParseResult.GetValueForOption(serverOption) ?? "127.0.0.1";
            var port = ctx.ParseResult.GetValueForOption(portOption);
            var clients = ctx.ParseResult.GetValueForOption(clientsOption);
            var aggInterval = ctx.ParseResult.GetValueForOption(aggIntervalOption);
            var rampMs = Math.Max(0, ctx.ParseResult.GetValueForOption(rampMsOption));
            var duration = ctx.ParseResult.GetValueForOption(durationOption);
            var movementRate = ctx.ParseResult.GetValueForOption(movementRateOption);
            var chatRate = ctx.ParseResult.GetValueForOption(chatRateOption);
            var gameEventRate = ctx.ParseResult.GetValueForOption(gameEventRateOption);
            var pingRate = ctx.ParseResult.GetValueForOption(pingRateOption);
            var payloadSize = ctx.ParseResult.GetValueForOption(payloadSizeOption);
            var broadcastCount = ctx.ParseResult.GetValueForOption(broadcastCountOption);
            var jitter = ctx.ParseResult.GetValueForOption(jitterOption);
            var fileLogs = ctx.ParseResult.GetValueForOption(fileLogsOption);

            var timestamp = DateTime.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
            var logDirectory = Path.Combine("logs", "clients", timestamp);
            if (fileLogs) Directory.CreateDirectory(logDirectory);

            var logConfig = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}");

            if (fileLogs)
            {
                logConfig = logConfig.WriteTo.File(
                    Path.Combine(logDirectory, "client-{Date}.log"),
                    rollingInterval: RollingInterval.Day,
                    outputTemplate: "[{Timestamp:yyyy-MM-dd HH:mm:ss.fff} {Level:u3}] {SourceContext}: {Message:lj}{NewLine}{Exception}",
                    buffered: true,
                    flushToDiskInterval: TimeSpan.FromSeconds(2)
                );
            }

            Log.Logger = logConfig.CreateLogger();

            var messageConfigs = new List<MessageTypeConfig>
            {
                new() { Type = MessageType.Movement,  PayloadSize = payloadSize,                                       RatePerSecond = movementRate,  Enabled = true },
                new() { Type = MessageType.Chat,      PayloadSize = StreamHeader.PayloadSize(MessageType.Chat),       RatePerSecond = chatRate,      Enabled = true },
                new() { Type = MessageType.GameEvent, PayloadSize = StreamHeader.PayloadSize(MessageType.GameEvent),  RatePerSecond = gameEventRate, Enabled = true },
                new() { Type = MessageType.Ping,      PayloadSize = StreamHeader.PayloadSize(MessageType.Ping),       RatePerSecond = pingRate,      Enabled = true }
            };

            // Resolve hostname or IP safely (fixes CS8604 and supports DNS)
            IPAddress ipAddress;
            if (!IPAddress.TryParse(server == "localhost" ? "127.0.0.1" : server, out ipAddress))
            {
                var addrs = await Dns.GetHostAddressesAsync(server);
                ipAddress = addrs.First(a => a.AddressFamily == AddressFamily.InterNetwork);
            }
            var serverEndPoint = new IPEndPoint(ipAddress, port);

            Log.Information("Starting {ClientCount} clients to {Server}:{Port} for {Duration}s (broadcast={Broadcast}, jitter={Jitter:P0}, agg={Agg}s)",
                clients, server, port, duration, broadcastCount, jitter, aggInterval);

            var testClients = new List<GameTestClient>(clients);
            var connectTasks = new List<Task>(clients);

            CancellationTokenSource? aggCts = null;
            Task? aggTask = null;

            // helpers (Humanizer)
            static string MsgRate(double v) => v.ToMetric(null, 2) + " msg/s";
            static string BytesPerSec(double v) => Humanizer.Bytes.ByteSize.FromBytes(v).ToString("0.0") + "/s";

            try
            {
                // Stagger connects to reduce startup timeouts under heavy load
                for (int i = 0; i < clients; i++)
                {
                    var clientId = i + 1;
                    var clientLogger = Log.Logger.ForContext("SourceContext", $"Client{clientId:D3}");
                    var client = new GameTestClient(clientLogger, messageConfigs, clientId, broadcastCount, jitter);
                    testClients.Add(client);

                    connectTasks.Add(client.ConnectAsync(serverEndPoint));

                    if (rampMs > 0 && i < clients - 1)
                        await Task.Delay(rampMs);
                }

                await Task.WhenAll(connectTasks);
                Log.Information("All {ClientCount} clients connected", clients);

                // Aggregate stats loop (with EMA smoothing)
                if (aggInterval > 0)
                {
                    aggCts = new CancellationTokenSource();
                    var interval = TimeSpan.FromSeconds(Math.Max(1, aggInterval));
                    var aggLogger = Log.ForContext("SourceContext", "AGG");

                    aggTask = Task.Run(async () =>
                    {
                        long lastSent = 0, lastRecv = 0, lastSB = 0, lastRB = 0;
                        bool first = true;

                        // EMA state
                        double emaSent = 0, emaRecv = 0, emaSB = 0, emaRB = 0;
                        const double alpha = 0.3; // smoothing factor

                        var token = aggCts.Token;

                        while (!token.IsCancellationRequested)
                        {
                            try { await Task.Delay(interval, token); } catch (OperationCanceledException) { break; }

                            long sent = 0, recv = 0, sb = 0, rb = 0;
                            foreach (var c in testClients)
                            {
                                var t = c.ReadTotals();
                                sent += t.sent; sb += t.sentBytes; recv += t.recv; rb += t.recvBytes;
                            }

                            if (first)
                            {
                                lastSent = sent; lastRecv = recv; lastSB = sb; lastRB = rb;
                                first = false;
                                continue;
                            }

                            var secs = interval.TotalSeconds;
                            var sentRate = (sent - lastSent) / secs;
                            var recvRate = (recv - lastRecv) / secs;
                            var sentBps  = (sb   - lastSB)   / secs;
                            var recvBps  = (rb   - lastRB)   / secs;

                            // update EMA
                            emaSent = emaSent == 0 ? sentRate : alpha * sentRate + (1 - alpha) * emaSent;
                            emaRecv = emaRecv == 0 ? recvRate : alpha * recvRate + (1 - alpha) * emaRecv;
                            emaSB   = emaSB   == 0 ? sentBps  : alpha * sentBps  + (1 - alpha) * emaSB;
                            emaRB   = emaRB   == 0 ? recvBps  : alpha * recvBps  + (1 - alpha) * emaRB;

                            aggLogger.Information("[AGG] Sent {InstSent} ({InstSBps}), Recv {InstRecv} ({InstRBps}) | EMA Sent {EmaSent} ({EmaSBps}), EMA Recv {EmaRecv} ({EmaRBps})",
                                MsgRate(sentRate), BytesPerSec(sentBps),
                                MsgRate(recvRate), BytesPerSec(recvBps),
                                MsgRate(emaSent),  BytesPerSec(emaSB),
                                MsgRate(emaRecv),  BytesPerSec(emaRB));

                            lastSent = sent; lastRecv = recv; lastSB = sb; lastRB = rb;
                        }
                    }, aggCts.Token);
                }

                await Task.Delay(TimeSpan.FromSeconds(duration));
                Log.Information("Test complete");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error during test");
            }
            finally
            {
                if (aggCts is not null) aggCts.Cancel();
                try { if (aggTask is not null) await aggTask; } catch { }

                foreach (var client in testClients)
                    client.Dispose();

                Log.CloseAndFlush();
            }
        });

        return await root.InvokeAsync(args);
    }
}


#endregion
