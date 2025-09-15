using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Serilog;
using Serilog.Events;
using Humanizer;
using Humanizer.Bytes;

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

#region Payload pool with ref-count

sealed class PayloadBlock
{
    public byte[] Buffer { get; }
    public int Length { get; }
    private int _refs;

    private PayloadBlock(byte[] buffer, int length) { Buffer = buffer; Length = length; _refs = 1; }

    public static PayloadBlock Rent(int length)
    {
        var buf = ArrayPool<byte>.Shared.Rent(length);
        return new PayloadBlock(buf, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddRefs(int delta)
    {
        if (delta == 0) return;
        Interlocked.Add(ref _refs, delta);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Release()
    {
        if (Interlocked.Decrement(ref _refs) == 0)
            ArrayPool<byte>.Shared.Return(Buffer);
    }
}

#endregion

#region Dispatch envelopes

readonly struct BroadcastEnvelope
{
    public readonly int SenderClientId;
    public readonly MessageType Type;
    public readonly ushort FixedPayloadSize;
    public readonly PayloadBlock Payload; // contains [MessageFrame + payload bytes]

    public BroadcastEnvelope(int senderId, MessageType type, ushort fixedSize, PayloadBlock payload)
    {
        SenderClientId = senderId;
        Type = type;
        FixedPayloadSize = fixedSize;
        Payload = payload;
    }

    public ReadOnlySpan<byte> Span => Payload.Buffer.AsSpan(0, Payload.Length);

    public ushort BroadcastCount
    {
        get
        {
            // First ushort of MessageFrame (packed) at offset 0
            return MemoryMarshal.Read<ushort>(Payload.Buffer.AsSpan(0, sizeof(ushort)));
        }
    }
}

readonly struct OutgoingItem
{
    public readonly MessageType Type;
    public readonly ushort FixedPayloadSize;
    public readonly PayloadBlock Payload;

    public OutgoingItem(MessageType t, ushort size, PayloadBlock p)
    {
        Type = t; FixedPayloadSize = size; Payload = p;
    }
}

#endregion

#region Perf

sealed class PerfCounters
{
    // dispatch queue
    long _dispatchQueued, _dispatchDrops, _dispatchDepth, _dispatchHigh;

    // inbound
    long _inMsgs, _inBytes, _inLatencySum, _inLatencyMax;

    // fanout
    long _fanoutRequests, _fanoutPlannedRecipients, _fanoutEnqOk, _fanoutEnqDrops;

    // outbound (measured at senders)
    long _outMsgs, _outBytes, _outBatches, _outWriteUsSum, _outWriteUsMax, _outStalls;

    long _stallThresholdUs = 1000; 

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnDispatchQueued()
    {
        Interlocked.Increment(ref _dispatchQueued);
        var d = Interlocked.Increment(ref _dispatchDepth);
        UpdateMax(ref _dispatchHigh, d);
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnDispatchDequeue() => Interlocked.Decrement(ref _dispatchDepth);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnDispatchDrop() => Interlocked.Increment(ref _dispatchDrops);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnInbound(int bytes, int latencyMs)
    {
        Interlocked.Increment(ref _inMsgs);
        Interlocked.Add(ref _inBytes, bytes);
        Interlocked.Add(ref _inLatencySum, latencyMs);
        UpdateMax(ref _inLatencyMax, latencyMs);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnFanoutPlanned(int plannedRecipients)
    {
        Interlocked.Increment(ref _fanoutRequests);
        Interlocked.Add(ref _fanoutPlannedRecipients, plannedRecipients);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnFanoutEnqueueSuccess() => Interlocked.Increment(ref _fanoutEnqOk);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnFanoutEnqueueDrop()    => Interlocked.Increment(ref _fanoutEnqDrops);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetStallThresholdMs(int ms) => _stallThresholdUs = Math.Max(0, ms) * 1000;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnOutbound(int msgCount, int bytes, double writeMs)
    {
        if (msgCount <= 0 || bytes <= 0) return;
        Interlocked.Add(ref _outMsgs, msgCount);
        Interlocked.Add(ref _outBytes, bytes);
        Interlocked.Increment(ref _outBatches);
        var us = (long)Math.Round(writeMs * 1000.0);
        Interlocked.Add(ref _outWriteUsSum, us);
        UpdateMax(ref _outWriteUsMax, us);
        if (writeMs * 1000.0 >= Interlocked.Read(ref _stallThresholdUs))
            Interlocked.Increment(ref _outStalls);
    }

    static void UpdateMax(ref long loc, long val)
    {
        long cur;
        while (val > (cur = Volatile.Read(ref loc)) &&
               Interlocked.CompareExchange(ref loc, val, cur) != cur) { }
    }

    // snapshot for logging (atomic reads)
    public Snapshot GetSnapshot() => new Snapshot(
        dispatchQueued: Volatile.Read(ref _dispatchQueued),
        dispatchDrops: Volatile.Read(ref _dispatchDrops),
        dispatchDepth: Volatile.Read(ref _dispatchDepth),
        dispatchHigh:  Volatile.Read(ref _dispatchHigh),
        inMsgs:  Volatile.Read(ref _inMsgs),
        inBytes: Volatile.Read(ref _inBytes),
        inLatencySum: Volatile.Read(ref _inLatencySum),
        inLatencyMax: Volatile.Read(ref _inLatencyMax),
        fanoutReq: Volatile.Read(ref _fanoutRequests),
        fanoutPlanned: Volatile.Read(ref _fanoutPlannedRecipients),
        fanoutOk: Volatile.Read(ref _fanoutEnqOk),
        fanoutDrops: Volatile.Read(ref _fanoutEnqDrops),
        outMsgs: Volatile.Read(ref _outMsgs),
        outBytes: Volatile.Read(ref _outBytes),
        outBatches: Volatile.Read(ref _outBatches),
        outWriteUsSum: Volatile.Read(ref _outWriteUsSum),
        outWriteUsMax: Volatile.Read(ref _outWriteUsMax),
        outStalls: Volatile.Read(ref _outStalls)
    );

    public readonly record struct Snapshot(
        long dispatchQueued, long dispatchDrops, long dispatchDepth, long dispatchHigh,
        long inMsgs, long inBytes, long inLatencySum, long inLatencyMax,
        long fanoutReq, long fanoutPlanned, long fanoutOk, long fanoutDrops,
        long outMsgs, long outBytes, long outBatches, long outWriteUsSum, long outWriteUsMax, long outStalls);
}

#endregion

#region Client session

sealed class ConnState {
    public QuicConnection Conn { get; }
    public TaskCompletionSource Ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public volatile bool IsReady;
    public int ClientId;
    public ConnState(QuicConnection c) => Conn = c;
}


sealed class ClientSession : IAsyncDisposable
{
    const int MAX_BATCH_BYTES = 64 * 1024; // cap batch to ~64KB to reduce syscalls
    const int MAX_BATCH_COUNT = 64;        // and a sane upper bound on messages per batch

    public int Id { get; }
    readonly QuicConnection _conn;
    readonly Channel<OutgoingItem> _outQueue;
    readonly CancellationTokenSource _cts = new();
    readonly Task _senderTask;
    readonly Task _inboundAcceptorTask;

    int _outDepth;  // current items in queue
    int _outHigh;   // high-water mark

    public int OutDepth => Volatile.Read(ref _outDepth);
    public int OutHigh  => Volatile.Read(ref _outHigh);

    // One outbound unidirectional stream per MessageType (payload size fixed per type)
    readonly Dictionary<MessageType, (QuicStream stream, ushort size)> _outStreams = new();

    volatile int _closed = 0;
    public bool IsConnected => _closed == 0 && !_cts.IsCancellationRequested;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void MarkClosed() => Interlocked.Exchange(ref _closed, 1);

    readonly QuicRelayServer _server;
    public PerfCounters Perf => _server.Perf;

    // stash for coalescer (if we pop a different-type item while batching)
    OutgoingItem _stash;
    bool _hasStash;

    readonly int _lingerMs; // micro-linger per batch

    public ClientSession(int id, QuicConnection conn, QuicRelayServer server, int perClientCap, int lingerMs)
    {
        Id = id;
        _conn = conn;
        _server = server;
        _lingerMs = Math.Max(0, lingerMs);

        _outQueue = Channel.CreateBounded<OutgoingItem>(new BoundedChannelOptions(perClientCap)
        {
            FullMode = BoundedChannelFullMode.DropWrite, // honest backpressure
            SingleReader = true,
            SingleWriter = false
        });

        _senderTask = Task.Run(SendLoopAsync);
        _inboundAcceptorTask = Task.Run(AcceptInboundStreamsLoopAsync);
    }

    public bool TryEnqueue(OutgoingItem item)
    {
        var ok = _outQueue.Writer.TryWrite(item);
        if (ok)
        {
            var d = Interlocked.Increment(ref _outDepth);
            int prev;
            while (d > (prev = Volatile.Read(ref _outHigh)) &&
                   Interlocked.CompareExchange(ref _outHigh, d, prev) != prev) { /* spin */ }
        }
        return ok;
    }

    async Task SendLoopAsync()
    {
        var batchBuffer = ArrayPool<byte>.Shared.Rent(MAX_BATCH_BYTES);
        try
        {
            while (await _outQueue.Reader.WaitToReadAsync(_cts.Token))
            {
                // get first item (from stash or queue)
                OutgoingItem first;
                if (_hasStash)
                {
                    first = _stash;
                    _hasStash = false;
                }
                else
                {
                    if (!_outQueue.Reader.TryRead(out first))
                        continue;
                }

                // resolve stream for the batch
                QuicStream stream;
                try
                {
                    stream = await GetOrCreateOutboundStreamAsync(first.Type, first.FixedPayloadSize, _cts.Token);
                }
                catch
                {
                    Interlocked.Decrement(ref _outDepth);
                    first.Payload.Release();
                    continue;
                }

                var msgSize = first.Payload.Length;
                int written = 0;
                int msgCount = 0;

                // copy first
                Buffer.BlockCopy(first.Payload.Buffer, 0, batchBuffer, 0, msgSize);
                written += msgSize;
                msgCount++;
                Interlocked.Decrement(ref _outDepth);
                first.Payload.Release();

                // linger deadline
                var useLinger = _lingerMs > 0;
                var deadlineUtc = useLinger ? DateTime.UtcNow.AddMilliseconds(_lingerMs) : DateTime.MinValue;

                if (useLinger)
                {
                    while (DateTime.UtcNow < deadlineUtc && msgCount < MAX_BATCH_COUNT && (written + msgSize) <= MAX_BATCH_BYTES)
                    {
                        if (_outQueue.Reader.TryRead(out var more))
                        {
                            if (more.Type != first.Type || more.FixedPayloadSize != first.FixedPayloadSize)
                            { _stash = more; _hasStash = true; break; }

                            Buffer.BlockCopy(more.Payload.Buffer, 0, batchBuffer, written, more.Payload.Length);
                            written += more.Payload.Length;
                            msgCount++;
                            Interlocked.Decrement(ref _outDepth);
                            more.Payload.Release();
                            continue;
                        }

                        var remaining = deadlineUtc - DateTime.UtcNow;
                        if (remaining <= TimeSpan.Zero) break;

                        var waitTask = _outQueue.Reader.WaitToReadAsync(_cts.Token).AsTask();
                        var delayTask = Task.Delay(remaining, _cts.Token);
                        var completed = await Task.WhenAny(waitTask, delayTask);

                        if (completed == delayTask) break; // deadline hit

                        bool ready;
                        try { ready = await waitTask; }     // ⚠ never use .Result on a possibly-canceled task
                        catch (OperationCanceledException) { ready = false; }
                        if (!ready) break; // channel completed/canceled; exit linger
                        // loop; TryRead again
                    }
                }

                // single write
                var t0 = Stopwatch.GetTimestamp();
                try
                {
                    await stream.WriteAsync(batchBuffer.AsMemory(0, written), _cts.Token);
                    // no explicit Flush; MsQuic packs frames efficiently
                }
                catch (QuicException)
                {
                    MarkClosed();
                }
                catch
                {
                    // swallow per-batch
                }
                finally
                {
                    var ms = Stopwatch.GetElapsedTime(t0).TotalMilliseconds;
                    Perf.OnOutbound(msgCount, written, ms);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.Warning(ex, "[SendLoop] Client {Id} error", Id);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(batchBuffer);
        }
    }

    async ValueTask<QuicStream> GetOrCreateOutboundStreamAsync(MessageType type, ushort fixedPayloadSize, CancellationToken ct)
    {
        if (_outStreams.TryGetValue(type, out var tuple))
        {
            if (tuple.stream.CanWrite && tuple.size == fixedPayloadSize)
                return tuple.stream;

            try { tuple.stream.Dispose(); } catch { }
            _outStreams.Remove(type);
        }

        var stream = await _conn.OpenOutboundStreamAsync(QuicStreamType.Unidirectional, ct);
        var header = new StreamHeader
        {
            StreamType = type,
            FixedPayloadSize = fixedPayloadSize,
            MessagePriority = TypeToPriority(type)
        };
        Span<byte> hdr = stackalloc byte[StreamHeader.Size];
        MemoryMarshal.Write(hdr, in header);
        await stream.WriteAsync(hdr.ToArray(), ct);

        _outStreams[type] = (stream, fixedPayloadSize);
        return stream;
    }

    static Priority TypeToPriority(MessageType t) => t switch
    {
        MessageType.Ping => Priority.Critical,
        MessageType.Movement => Priority.High,
        MessageType.GameEvent => Priority.Normal,
        MessageType.Chat => Priority.Normal,
        _ => Priority.Normal
    };

    async Task AcceptInboundStreamsLoopAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                QuicStream stream;
                try
                {
                    stream = await _conn.AcceptInboundStreamAsync(_cts.Token);
                }
                catch (QuicException) { MarkClosed(); break; }
                catch (OperationCanceledException) { break; }

                _ = Task.Run(() => HandleInboundStreamAsync(stream));
            }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[InboundAcceptor] Client {Id} error", Id);
        }
    }

    async Task HandleInboundStreamAsync(QuicStream stream)
    {
        await using (stream)
        {
            try
            {
                // read StreamHeader once
                var headerBuf = new byte[StreamHeader.Size];
                if (!await ReadExactAsync(stream, headerBuf, StreamHeader.Size, _cts.Token))
                    return;

                var header = MemoryMarshal.Read<StreamHeader>(headerBuf);
                var messageSize = MessageFrame.Size + header.FixedPayloadSize;

                // read directly into pooled payload block (no extra copy)
                while (!_cts.IsCancellationRequested)
                {
                    var block = PayloadBlock.Rent(messageSize);
                    int read = 0, zeroReads = 0;

                    while (read < messageSize)
                    {
                        var n = await stream.ReadAsync(block.Buffer.AsMemory(read, messageSize - read), _cts.Token);
                        if (n == 0)
                        {
                            if (stream.ReadsClosed.IsCompleted) { block.Release(); return; }
                            if (++zeroReads >= 3) { block.Release(); break; }
                            await Task.Delay(1, _cts.Token);
                            continue;
                        }
                        zeroReads = 0;
                        read += n;
                    }
                    if (read < messageSize) { block.Release(); break; }

                    var frame = MemoryMarshal.Read<MessageFrame>(block.Buffer.AsSpan(0, MessageFrame.Size));
                    var latencyMs = (int)((DateTime.UtcNow.Ticks - frame.TimestampTicks) / TimeSpan.TicksPerMillisecond);
                    _server.Perf.OnInbound(messageSize, latencyMs);

                    var envelope = new BroadcastEnvelope(Id, header.StreamType, header.FixedPayloadSize, block);

                    if (!_server.TryDispatch(envelope))
                        block.Release();
                }
            }
            catch (OperationCanceledException) { }
            catch (QuicException) { }
            catch (Exception ex)
            {
                Log.Warning(ex, "[Inbound] Client {Id} stream {StreamId} error", Id, stream.Id);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        MarkClosed();
        try { _outQueue.Writer.TryComplete(); } catch { }
        _cts.Cancel();

        try { await _senderTask.ConfigureAwait(false); } catch { }
        try { await _inboundAcceptorTask.ConfigureAwait(false); } catch { }

        foreach (var kv in _outStreams.Values)
        {
            try { kv.stream.Dispose(); } catch { }
        }

        try { await _conn.DisposeAsync(); } catch { }
        _cts.Dispose();
    }

    static async Task<bool> ReadExactAsync(QuicStream s, byte[] buf, int count, CancellationToken ct)
    {
        int read = 0, zeroReads = 0;
        while (read < count)
        {
            var n = await s.ReadAsync(buf.AsMemory(read, count - read), ct);
            if (n == 0)
            {
                zeroReads++;
                if (s.ReadsClosed.IsCompleted) return false;
                if (zeroReads >= 3) return false;
                await Task.Delay(1, ct);
                continue;
            }
            zeroReads = 0;
            read += n;
        }
        return true;
    }
}

#endregion

#region Server

sealed class QuicRelayServer : IAsyncDisposable
{
    // Helper to read exactly buffer.Length bytes or throw
    private static async Task ReadExactlyAsync(QuicStream s, Memory<byte> buffer, CancellationToken ct)
    {
        int read = 0, zeroReads = 0;
        while (read < buffer.Length)
        {
            var n = await s.ReadAsync(buffer.Slice(read), ct);
            if (n == 0)
            {
                zeroReads++;
                if (s.ReadsClosed.IsCompleted) throw new EndOfStreamException();
                if (zeroReads >= 3) throw new EndOfStreamException();
                await Task.Delay(1, ct);
                continue;
            }
            zeroReads = 0;
            read += n;
        }
    }

    readonly int _maxFanout; // 0 = unlimited
    readonly Channel<BroadcastEnvelope> _dispatchChannel;
    readonly ConcurrentDictionary<int, ClientSession> _clients = new();
    readonly CancellationTokenSource _cts = new();

    readonly Task[] _dispatcherTasks;
    readonly Task _cleanupTask;
    Task? _acceptTask;

    QuicListener? _listener;
    int _nextId = 0;

    readonly PerfCounters _perf = new();
    Task? _statsTask;

    public PerfCounters Perf => _perf;

    // config
    readonly int _perClientQueueCap;
    readonly int _lingerMs;

    // CSV sink
    readonly string? _csvPath;
    StreamWriter? _csvWriter;
    bool _csvHeaderWritten;

    public QuicRelayServer(int dispatchCap, int perClientCap, int maxFanout, int dispatchWorkers, int lingerMs, string? csvPath)
    {
        _perClientQueueCap = perClientCap;
        _maxFanout = Math.Max(0, maxFanout);
        _lingerMs = Math.Max(0, lingerMs);
        _csvPath = string.IsNullOrWhiteSpace(csvPath) ? null : csvPath;

        _dispatchChannel = Channel.CreateBounded<BroadcastEnvelope>(new BoundedChannelOptions(dispatchCap)
        {
            FullMode = BoundedChannelFullMode.DropWrite, // honest backpressure
            SingleReader = false,
            SingleWriter = false
        });

        var workers = Math.Max(1, Math.Min(dispatchWorkers, Environment.ProcessorCount));
        _dispatcherTasks = new Task[workers];
        for (int i = 0; i < workers; i++)
            _dispatcherTasks[i] = Task.Run(DispatchLoopAsync);

        _cleanupTask = Task.Run(CleanupLoopAsync);
        _statsTask = Task.Run(StatsLoopAsync);
    }

    public bool TryDispatch(BroadcastEnvelope env)
    {
        var ok = _dispatchChannel.Writer.TryWrite(env);
        if (ok) _perf.OnDispatchQueued();
        else _perf.OnDispatchDrop();
        return ok;
    }

    async Task StatsLoopAsync()
    {
        var prev = _perf.GetSnapshot();
        try
        {
            // CSV writer (lazy open)
            if (_csvPath is not null)
            {
                var __dir = Path.GetDirectoryName(_csvPath);
                if (!string.IsNullOrEmpty(__dir)) Directory.CreateDirectory(__dir);
                _csvWriter = new StreamWriter(new FileStream(_csvPath, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 16, FileOptions.WriteThrough));
            }

            var __prevTimeUtc = DateTime.UtcNow;
            while (!_cts.IsCancellationRequested)
            {
                await Task.Delay(5000, _cts.Token);
                var __now = DateTime.UtcNow;
                var dt = (__now - __prevTimeUtc).TotalSeconds;
                if (dt <= 0) dt = 0.000001;
                __prevTimeUtc = __now;
                var snap = _perf.GetSnapshot();

                // dt measured precisely above
                var inMsgRate   = (snap.inMsgs   - prev.inMsgs)   / dt;
                var inKBps      = ((snap.inBytes - prev.inBytes)  / dt) / 1024.0;
                var outMsgRate  = (snap.outMsgs  - prev.outMsgs)  / dt;
                var outKBps     = ((snap.outBytes - prev.outBytes)/ dt) / 1024.0;
                var outBatchRate= (snap.outBatches - prev.outBatches) / dt;
                var stallsRate  = (snap.outStalls - prev.outStalls) / dt;

                var __inMsgDelta = (snap.inMsgs - prev.inMsgs);
                double avgLatency = __inMsgDelta > 0 ? (double)(snap.inLatencySum - prev.inLatencySum) / __inMsgDelta : 0.0;
                var __fanReqDelta = (snap.fanoutReq - prev.fanoutReq);
                double avgFanout  = __fanReqDelta > 0 ? (double)(snap.fanoutPlanned - prev.fanoutPlanned) / __fanReqDelta : 0.0;
                double avgBatchWriteMs = (snap.outBatches - prev.outBatches) > 0
                    ? ((snap.outWriteUsSum - prev.outWriteUsSum) / 1000.0) / (snap.outBatches - prev.outBatches)
                    : 0.0;
                double maxBatchWriteMs = (snap.outWriteUsMax) / 1000.0;

                int clients = _clients.Count;
                int curOutSum = 0, outHighMax = 0;
                foreach (var c in _clients.Values)
                {
                    curOutSum += c.OutDepth;
                    if (c.OutHigh > outHighMax) outHighMax = c.OutHigh;
                }

                var __inBps = (snap.inBytes - prev.inBytes) / dt;
                var __outBps = (snap.outBytes - prev.outBytes) / dt;
                var __inBpsHuman  = ByteSize.FromBytes(__inBps).ToString();
                var __outBpsHuman = ByteSize.FromBytes(__outBps).ToString();
                var __inRateHuman  = inMsgRate >= 1000 ? inMsgRate.ToMetric(decimals: 2) : inMsgRate.ToString("F1", CultureInfo.InvariantCulture);
                var __outRateHuman = outMsgRate >= 1000 ? outMsgRate.ToMetric(decimals: 2) : outMsgRate.ToString("F1", CultureInfo.InvariantCulture);
                Log.Information(
                    "[Stats] clients={Clients} | inbound={InRate} msg/s, {InBps}/s | " +
                    "egress={OutRate} msg/s, {OutBps}/s, batches/s={BatchRate:F1}, stalls/s={Stalls:F1} | " +
                    "dispatchQ depth={DDepth}, high={DHigh}, drops={DDropsDelta} | " +
                    "fanout avg={Fanout:F1}, enq/s={FanEnqPerSec:F1}, drops/s={FanDropsPerSec:F1} | " +
                    "latency avg={Latency:F1} ms | perClientQ sum={PerClientSum}, highMax={PerClientHighMax} | batch write avg={BatchAvg:F2} ms max={BatchMax:F2} ms",
                    clients, __inRateHuman, __inBpsHuman, __outRateHuman, __outBpsHuman, outBatchRate, stallsRate,
                    snap.dispatchDepth,
                    snap.dispatchHigh, (snap.dispatchDrops - prev.dispatchDrops),
                    avgFanout,
                    (snap.fanoutOk    - prev.fanoutOk)    / dt,
                    (snap.fanoutDrops - prev.fanoutDrops) / dt,
                    avgLatency, curOutSum, outHighMax,
                    avgBatchWriteMs, maxBatchWriteMs
                );

                // CSV row
                if (_csvWriter is not null)
                {
                    if (!_csvHeaderWritten)
                    {
                        _csvWriter.WriteLine(string.Join(',',
                            "timestamp","clients",
                            "in_msg_per_s","in_kb_per_s",
                            "out_msg_per_s","out_kb_per_s",
                            "batches_per_s","stalls_per_s",
                            "dispatch_depth","dispatch_high","dispatch_drops_delta",
                            "fanout_avg","fanout_enq_per_s","fanout_drops_per_s",
                            "latency_avg_ms","per_client_q_sum","per_client_q_highmax",
                            "batch_write_avg_ms","batch_write_max_ms"));
                        _csvHeaderWritten = true;
                    }

                    var row = string.Join(',', new[]
                    {
                        DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
                        clients.ToString(CultureInfo.InvariantCulture),
                        inMsgRate.ToString("F1", CultureInfo.InvariantCulture),
                        inKBps.ToString("F1", CultureInfo.InvariantCulture),
                        outMsgRate.ToString("F1", CultureInfo.InvariantCulture),
                        outKBps.ToString("F1", CultureInfo.InvariantCulture),
                        outBatchRate.ToString("F1", CultureInfo.InvariantCulture),
                        stallsRate.ToString("F1", CultureInfo.InvariantCulture),
                        snap.dispatchDepth.ToString(CultureInfo.InvariantCulture),
                        snap.dispatchHigh.ToString(CultureInfo.InvariantCulture),
                        (snap.dispatchDrops - prev.dispatchDrops).ToString(CultureInfo.InvariantCulture),
                        avgFanout.ToString("F1", CultureInfo.InvariantCulture),
                        (((double)(snap.fanoutOk - prev.fanoutOk))/dt).ToString("F1", CultureInfo.InvariantCulture),
                        (((double)(snap.fanoutDrops - prev.fanoutDrops))/dt).ToString("F1", CultureInfo.InvariantCulture),
                        avgLatency.ToString("F1", CultureInfo.InvariantCulture),
                        curOutSum.ToString(CultureInfo.InvariantCulture),
                        outHighMax.ToString(CultureInfo.InvariantCulture),
                        avgBatchWriteMs.ToString("F2", CultureInfo.InvariantCulture),
                        maxBatchWriteMs.ToString("F2", CultureInfo.InvariantCulture)
                    });

                    _csvWriter.WriteLine(row);
                    _csvWriter.Flush();
                }

                prev = snap;
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.Warning(ex, "[Stats] loop error");
        }
        finally
        {
            try { _csvWriter?.Flush(); } catch { }
            try { _csvWriter?.Dispose(); } catch { }
        }
    }

    public async Task StartAsync(IPEndPoint ep, int backlog, CancellationToken externalCt)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, externalCt);
        var ct = linked.Token;

        var serverConnOptions = new QuicServerConnectionOptions
        {
            DefaultStreamErrorCode = 0x0A,
            DefaultCloseErrorCode = 0x0B,
            ServerAuthenticationOptions = CreateServerAuth(),
            MaxInboundUnidirectionalStreams = 16384,
            MaxInboundBidirectionalStreams = 256,
            HandshakeTimeout = TimeSpan.FromSeconds(60),
            IdleTimeout = TimeSpan.FromMinutes(5),
        };

        var listenerOptions = new QuicListenerOptions
        {
            ListenEndPoint = ep,
            ApplicationProtocols = new List<SslApplicationProtocol> { new("game-relay") },
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(serverConnOptions),
            ListenBacklog = backlog
        };

        _listener = await QuicListener.ListenAsync(listenerOptions, ct);
        Log.Information("[Server] Listening on {Ip}:{Port}", ep.Address, ep.Port);

        _acceptTask = Task.Run(async () =>
        {
            try
            {
                // Light sampling for connection spam: log every Nth
                const int sample = 50;
                while (!ct.IsCancellationRequested)
                {
                    QuicConnection conn;
                    try
                    {
                        conn = await _listener.AcceptConnectionAsync(ct);
                    }
                    catch (OperationCanceledException) { break; }

                    var state = new ConnState(conn);
                    _ = Task.Run(() => HandleControlStreamAsync(state));
                    await state.Ready.Task; // <-- gate broadcast registration on this
                    // RegisterForBroadcast(state); // only after READY

                    var id = Interlocked.Increment(ref _nextId);
                    var session = new ClientSession(id, conn, this, _perClientQueueCap, _lingerMs);
                    _clients[id] = session;

                    if (id % sample == 0)
                        Log.Information("[Server] Clients connected: {Count}", _clients.Count);
                }
            }
            catch (QuicException ex) { Log.Warning(ex, "[Accept] QUIC error"); }
            catch (Exception ex) { Log.Warning(ex, "[Accept] error"); }
        }, ct);
    }

    private async Task HandleControlStreamAsync(ConnState s)
    {
        try
        {
            using var ctrl = await s.Conn.AcceptInboundStreamAsync();
            var hdr = new byte[12];
            await ReadExactlyAsync(ctrl, hdr, _cts.Token);
            if (Encoding.ASCII.GetString(hdr, 0, 4) != "RDY1")
                throw new InvalidDataException("Unexpected control preface");

            s.ClientId = BinaryPrimitives.ReadInt32LittleEndian(hdr.AsSpan(4));
            s.IsReady = true;
            s.Ready.TrySetResult();

            // small ACK
            await ctrl.WriteAsync("OK"u8.ToArray());
            ctrl.CompleteWrites();
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "Control stream error");
            s.Ready.TrySetException(ex);
        }
    }

    async Task DispatchLoopAsync()
    {
        try
        {
            await foreach (var env in _dispatchChannel.Reader.ReadAllAsync(_cts.Token))
            {
                _perf.OnDispatchDequeue();

                // build candidate id list into a rented buffer
                var total = _clients.Count;
                if (total <= 1) { env.Payload.Release(); continue; }

                var pool = ArrayPool<int>.Shared;
                var buf = pool.Rent(Math.Max(1, total - 1));
                try
                {
                    var k = PickRandomTargetsExceptSender(
                        env.SenderClientId, _clients, buf.AsSpan(0, total - 1),
                        requested: env.BroadcastCount, maxFanout: _maxFanout);

                    if (k <= 0) { env.Payload.Release(); continue; }

                    _perf.OnFanoutPlanned(k);
                    env.Payload.AddRefs(k);

                    for (int i = 0; i < k; i++)
                    {
                        var id = buf[i];
                        if (_clients.TryGetValue(id, out var sess) &&
                            sess.TryEnqueue(new OutgoingItem(env.Type, env.FixedPayloadSize, env.Payload)))
                        {
                            _perf.OnFanoutEnqueueSuccess();
                        }
                        else
                        {
                            _perf.OnFanoutEnqueueDrop();
                            env.Payload.Release();
                        }
                    }
                    env.Payload.Release();
                }
                finally
                {
                    pool.Return(buf, clearArray: false);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.Warning(ex, "[Dispatch] error");
        }
    }

    async Task CleanupLoopAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                await Task.Delay(1000, _cts.Token);

                int removed = 0;
                foreach (var kv in _clients)
                {
                    var sess = kv.Value;
                    if (!sess.IsConnected && _clients.TryRemove(kv.Key, out var s))
                    {
                        removed++;
                        _ = s.DisposeAsync();
                    }
                }
                if (removed > 0)
                    Log.Information("[Cleanup] Removed {Removed} clients, {Active} active", removed, _clients.Count);
            }
        }
        catch (OperationCanceledException) { }
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _dispatchChannel.Writer.TryComplete();

        if (_listener is not null)
        {
            try { await _listener.DisposeAsync(); } catch { }
        }

        if (_acceptTask is not null) { try { await _acceptTask; } catch { } }
        if (_statsTask  is not null) { try { await _statsTask;  } catch { } }

        foreach (var t in _dispatcherTasks)
            try { await t; } catch { }

        try { await _cleanupTask; } catch { }

        foreach (var kv in _clients)
            try { await kv.Value.DisposeAsync(); } catch { }

        try { _csvWriter?.Dispose(); } catch { }
        _cts.Dispose();
    }

    static SslServerAuthenticationOptions CreateServerAuth()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var req = new System.Security.Cryptography.X509Certificates.CertificateRequest(
            "CN=localhost", rsa, System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        var cert = req.CreateSelfSigned(DateTimeOffset.Now, DateTimeOffset.Now.AddYears(1));
        var exported = new X509Certificate2(cert.Export(X509ContentType.Pfx), "",
            X509KeyStorageFlags.Exportable);
        return new SslServerAuthenticationOptions
        {
            ApplicationProtocols = new List<SslApplicationProtocol> { new("game-relay") },
            ServerCertificate = exported,
            ClientCertificateRequired = false
        };
    }

    // Returns k uniformly-random distinct targets (excluding sender) into `scratch`
    static int PickRandomTargetsExceptSender(
        int senderId,
        ConcurrentDictionary<int, ClientSession> clients,
        Span<int> scratch,
        int requested,
        int maxFanout) // 0 = unlimited
    {
        var n = 0;
        foreach (var kv in clients)
        {
            if (kv.Key == senderId) continue;
            if (n >= scratch.Length) break; // guard vs. churn after Count snapshot
            scratch[n++] = kv.Key;
        }
        if (n == 0) return 0;

        var cap = maxFanout == 0 ? n : Math.Min(maxFanout, n);
        var want = Math.Clamp(requested, 0, cap);
        if (want == 0) return 0;

        for (int i = 0; i < want; i++)
        {
            int j = i + Random.Shared.Next(n - i); // [i, n)
            (scratch[i], scratch[j]) = (scratch[j], scratch[i]);
        }
        return want;
    }
}

#endregion

#region Program

public static class Program
{
    public static async Task Main(string[] args)
    {
        string ip = "0.0.0.0";
        int port = 4433;
        int backlog = 4096;
        int dispatchCap = 10000;
        int perClientCap = 2000; // consider 256 for tighter tails
        int maxFanout = 128;
        int dispatchWorkers = 4;
        int lingerMs = 0; // micro-linger off by default
        string? csvPath = null;
        int minWorkerThreads = Math.Max(16, Environment.ProcessorCount * 4);
        int stallThresholdMs = 1; 

        // poor-man arg parse
        for (int i = 0; i < args.Length - 1; i++)
        {
            switch (args[i])
            {
                case "--ip": ip = args[++i]; break;
                case "--port": port = int.Parse(args[++i]); break;
                case "--backlog": backlog = int.Parse(args[++i]); break;
                case "--dispatch-cap": dispatchCap = int.Parse(args[++i]); break;
                case "--per-client-cap": perClientCap = int.Parse(args[++i]); break;
                case "--max-fanout": maxFanout = int.Parse(args[++i]); break;
                case "--dispatch-workers": dispatchWorkers = int.Parse(args[++i]); break;
                case "--linger-ms": lingerMs = int.Parse(args[++i]); break;
                case "--csv": csvPath = args[++i]; break;
                case "--min-worker-threads": minWorkerThreads = int.Parse(args[++i]); break;
                case "--stall-ms": stallThresholdMs = int.Parse(args[++i]); break;
            }
        }

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Async(a => a.Console(outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}"))
            .CreateLogger();
            

        // Ensure accept/dispatch have runway under I/O pressure
        ThreadPool.GetMinThreads(out var curWorkers, out var curIO);
        if (minWorkerThreads > curWorkers)
        {
            ThreadPool.SetMinThreads(minWorkerThreads, curIO);
            Log.Information("[ThreadPool] Min worker threads set to {Min}", minWorkerThreads);
        }

        var cts = new CancellationTokenSource();
        int sigCount = 0;
        Console.CancelKeyPress += (s, e) =>
        {
            e.Cancel = true;
            if (Interlocked.Increment(ref sigCount) == 1)
            {
                Log.Information("Ctrl+C received, shutting down...");
                cts.Cancel();
            }
        };

        await using var server = new QuicRelayServer(dispatchCap, perClientCap, maxFanout, dispatchWorkers, lingerMs, csvPath);
        var ep = new IPEndPoint(IPAddress.Parse(ip), port);

        server.Perf.SetStallThresholdMs(stallThresholdMs);
        Log.Information("[Server] Ready (backlog={Backlog}, perClientCap={PerClient}, maxFanout={Fanout}, lingerMs={Linger}, stallMs={Stall})",
            backlog, perClientCap, maxFanout, lingerMs, stallThresholdMs);
        try
        {
            await server.StartAsync(ep, backlog, cts.Token);
            Log.Information("[Server] Press Ctrl+C to stop");
            await Task.Delay(Timeout.Infinite, cts.Token);
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Log.Error(ex, "[Main] Fatal");
        }

        Log.Information("[Main] Shutdown complete");
        Log.CloseAndFlush();
    }
}

#endregion
