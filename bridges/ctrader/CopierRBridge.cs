using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using cAlgo.API;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.FullAccess)]
    public class CopierRBridge : Robot
    {
        [Parameter("Host", DefaultValue = "127.0.0.1")]
        public string Host { get; set; }

        [Parameter("Port", DefaultValue = 48100)]
        public int Port { get; set; }

        [Parameter("Account Id", DefaultValue = "ctrader-account")]
        public string AccountId { get; set; }

        [Parameter("Token", DefaultValue = "replace-me")]
        public string Token { get; set; }

        [Parameter("Publish Events", DefaultValue = true)]
        public bool PublishEvents { get; set; }

        private TcpClient _client;
        private StreamReader _reader;
        private StreamWriter _writer;
        private CancellationTokenSource _cts;
        private readonly object _writerLock = new object();
        private readonly Dictionary<long, double> _lastVolumeLots = new Dictionary<long, double>();

        protected override void OnStart()
        {
            Positions.Opened += OnPositionOpened;
            Positions.Modified += OnPositionModified;
            Positions.Closed += OnPositionClosed;

            _client = new TcpClient { NoDelay = true };
            _client.Connect(Host, Port);
            var stream = _client.GetStream();
            _reader = new StreamReader(stream);
            _writer = new StreamWriter(stream) { AutoFlush = true, NewLine = "\n" };
            _cts = new CancellationTokenSource();

            SendLine($"HELLO\t1\t{Clean(AccountId)}\tctrader\t{Clean(Token)}");
            Task.Run(ReadLoop);

            foreach (var position in Positions)
                _lastVolumeLots[position.Id] = Lots(position);
        }

        protected override void OnStop()
        {
            Positions.Opened -= OnPositionOpened;
            Positions.Modified -= OnPositionModified;
            Positions.Closed -= OnPositionClosed;
            _cts?.Cancel();
            _reader?.Dispose();
            _writer?.Dispose();
            _client?.Close();
        }

        private async Task ReadLoop()
        {
            try
            {
                while (!_cts.IsCancellationRequested)
                {
                    var line = await _reader.ReadLineAsync();
                    if (line == null)
                        break;
                    if (line.StartsWith("COMMAND\t", StringComparison.Ordinal))
                        BeginInvokeOnMainThread(() => HandleCommand(line));
                }
            }
            catch (Exception error)
            {
                Print("CopierR reader stopped: {0}", error.Message);
            }
        }

        private void OnPositionOpened(PositionOpenedEventArgs args)
        {
            var position = args.Position;
            _lastVolumeLots[position.Id] = Lots(position);
            if (!PublishEvents || IsCopied(position))
                return;
            SendPositionEvent("open", position, Lots(position), Lots(position));
        }

        private void OnPositionModified(PositionModifiedEventArgs args)
        {
            var position = args.Position;
            var current = Lots(position);
            _lastVolumeLots.TryGetValue(position.Id, out var previous);
            _lastVolumeLots[position.Id] = current;
            if (!PublishEvents || IsCopied(position))
                return;

            if (previous > 0 && current < previous)
                SendPositionEvent("reduce", position, previous - current, current);
            else
                SendPositionEvent("modify", position, current, current);
        }

        private void OnPositionClosed(PositionClosedEventArgs args)
        {
            var position = args.Position;
            _lastVolumeLots.TryGetValue(position.Id, out var previous);
            _lastVolumeLots.Remove(position.Id);
            if (!PublishEvents || IsCopied(position))
                return;
            SendPositionEvent("close", position, previous > 0 ? previous : Lots(position), 0.0);
        }

        private void SendPositionEvent(string action, Position position, double eventVolume, double remainingVolume)
        {
            var eventId = string.Format(CultureInfo.InvariantCulture,
                "{0}:{1}:{2}:{3:0.########}:{4:0.########}:{5}:{6}",
                AccountId, position.Id, action, eventVolume, remainingVolume,
                F(position.StopLoss), F(position.TakeProfit));
            var side = position.TradeType == TradeType.Buy ? "buy" : "sell";
            var line = string.Join("\t", new[]
            {
                "EVENT", "1", Clean(eventId), Clean(AccountId), "ctrader", action,
                position.Id.ToString(CultureInfo.InvariantCulture), Clean(position.SymbolName), side,
                eventVolume.ToString("0.########", CultureInfo.InvariantCulture),
                remainingVolume.ToString("0.########", CultureInfo.InvariantCulture),
                position.EntryPrice.ToString("0.########", CultureInfo.InvariantCulture),
                F(position.StopLoss), F(position.TakeProfit), NowNs().ToString(CultureInfo.InvariantCulture), ""
            });
            SendLine(line);
        }

        private void HandleCommand(string line)
        {
            var parts = line.Split('\t');
            if (parts.Length != 18 || parts[0] != "COMMAND" || parts[1] != "1" || parts[7] != AccountId)
                return;

            var commandId = parts[2];
            var action = parts[8];
            var targetOrderId = ParseLong(parts[9]);
            var symbolName = parts[10];
            var side = parts[11];
            var lots = ParseDouble(parts[12]);
            var sl = ParseNullableDouble(parts[16]);
            var tp = ParseNullableDouble(parts[17]);

            try
            {
                if (action == "open")
                {
                    var symbol = Symbols.GetSymbol(symbolName);
                    if (symbol == null)
                    {
                        SendAck(commandId, "rejected", "", "unknown symbol");
                        return;
                    }
                    var volume = symbol.NormalizeVolumeInUnits(symbol.QuantityToVolumeInUnits(lots), RoundingMode.ToNearest);
                    var tradeType = side == "buy" ? TradeType.Buy : TradeType.Sell;
                    var result = ExecuteMarketOrder(tradeType, symbolName, volume, "CopierR:" + commandId);
                    if (!result.IsSuccessful || result.Position == null)
                    {
                        SendAck(commandId, "rejected", "", result.Error.ToString());
                        return;
                    }
                    if (sl.HasValue || tp.HasValue)
                        ModifyPosition(result.Position, sl, tp);
                    SendAck(commandId, "filled", result.Position.Id.ToString(CultureInfo.InvariantCulture), "");
                    return;
                }

                var position = Positions.FirstOrDefault(p => p.Id == targetOrderId);
                if (position == null)
                {
                    SendAck(commandId, "rejected", "", "target position not found");
                    return;
                }

                TradeResult commandResult;
                if (action == "modify")
                {
                    commandResult = ModifyPosition(position, sl, tp);
                }
                else if (action == "reduce")
                {
                    var symbol = Symbols.GetSymbol(position.SymbolName);
                    var volume = symbol.NormalizeVolumeInUnits(symbol.QuantityToVolumeInUnits(lots), RoundingMode.ToNearest);
                    commandResult = ClosePosition(position, volume);
                }
                else if (action == "close")
                {
                    commandResult = ClosePosition(position);
                }
                else
                {
                    SendAck(commandId, "rejected", "", "unknown action");
                    return;
                }

                SendAck(commandId,
                    commandResult.IsSuccessful ? "filled" : "rejected",
                    targetOrderId.ToString(CultureInfo.InvariantCulture),
                    commandResult.IsSuccessful ? "" : commandResult.Error.ToString());
            }
            catch (Exception error)
            {
                SendAck(commandId, "unknown", "", error.Message);
            }
        }

        private void SendAck(string commandId, string status, string externalId, string message)
        {
            SendLine(string.Join("\t", new[]
            {
                "ACK", "1", Clean(commandId), Clean(AccountId), status, Clean(externalId),
                NowNs().ToString(CultureInfo.InvariantCulture), Clean(message)
            }));
        }

        private void SendLine(string line)
        {
            lock (_writerLock)
            {
                _writer?.WriteLine(line);
            }
        }

        private bool IsCopied(Position position)
        {
            return !string.IsNullOrEmpty(position.Label) &&
                   position.Label.StartsWith("CopierR:", StringComparison.Ordinal);
        }

        private double Lots(Position position)
        {
            var symbol = Symbols.GetSymbol(position.SymbolName);
            return symbol.VolumeInUnitsToQuantity(position.VolumeInUnits);
        }

        private static long NowNs()
        {
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1_000_000L;
        }

        private static string F(double? value)
        {
            return value.HasValue
                ? value.Value.ToString("0.########", CultureInfo.InvariantCulture)
                : "";
        }

        private static double ParseDouble(string value)
        {
            return string.IsNullOrEmpty(value) ? 0.0 : double.Parse(value, CultureInfo.InvariantCulture);
        }

        private static double? ParseNullableDouble(string value)
        {
            return string.IsNullOrEmpty(value) ? (double?)null : double.Parse(value, CultureInfo.InvariantCulture);
        }

        private static long ParseLong(string value)
        {
            return string.IsNullOrEmpty(value) ? 0L : long.Parse(value, CultureInfo.InvariantCulture);
        }

        private static string Clean(string value)
        {
            return (value ?? "").Replace('\t', ' ').Replace('\r', ' ').Replace('\n', ' ');
        }
    }
}
