using System.Collections.Concurrent;
using SolaceSystems.Solclient.Messaging;

namespace messaging_lab.solace.fanout;

/// <summary>
/// Records the broker's verdict on each guaranteed message. <see cref="ISession.Send(IMessage)"/>
/// returning OK only means the message was accepted for transmission: the ACK or rejection arrives
/// later as a session event, so this is the only place a rejected publish is visible at all.
/// </summary>
public sealed class PublishOutcomeTracker
{
    long _sent;
    long _acked;
    long _rejected;
    long _sendFailures;
    long _firstRejectedTicks;
    long _lastRejectedTicks;
    long _lastAckedTicks;

    public ConcurrentQueue<long> RejectedSequences { get; } = new();
    public ConcurrentDictionary<string, long> RejectionReasons { get; } = new();

    public long Sent => Interlocked.Read(ref _sent);
    public long Acked => Interlocked.Read(ref _acked);
    public long Rejected => Interlocked.Read(ref _rejected);
    public long SendFailures => Interlocked.Read(ref _sendFailures);
    public long Outstanding => Sent - Acked - Rejected;

    public DateTime? FirstRejectedUtc => ToUtc(Interlocked.Read(ref _firstRejectedTicks));
    public DateTime? LastRejectedUtc => ToUtc(Interlocked.Read(ref _lastRejectedTicks));
    public DateTime? LastAckedUtc => ToUtc(Interlocked.Read(ref _lastAckedTicks));

    public void RecordSent() => Interlocked.Increment(ref _sent);

    public void RecordSendFailure() => Interlocked.Increment(ref _sendFailures);

    /// <summary>Runs on the context thread, so it only updates counters - no logging or I/O.</summary>
    public void OnSessionEvent(SessionEventArgs args)
    {
        var nowTicks = DateTime.UtcNow.Ticks;

        switch (args.Event)
        {
            case SessionEvent.Acknowledgement:
                Interlocked.Increment(ref _acked);
                Interlocked.Exchange(ref _lastAckedTicks, nowTicks);
                break;

            case SessionEvent.RejectedMessageError:
                Interlocked.Increment(ref _rejected);
                Interlocked.CompareExchange(ref _firstRejectedTicks, nowTicks, 0);
                Interlocked.Exchange(ref _lastRejectedTicks, nowTicks);
                if (args.CorrelationKey is long sequence) RejectedSequences.Enqueue(sequence);
                RejectionReasons.AddOrUpdate($"{args.ResponseCode}: {args.Info}", 1, (_, count) => count + 1);
                break;
        }
    }

    static DateTime? ToUtc(long ticks) => ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
}
