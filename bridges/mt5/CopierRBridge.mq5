#property strict
#property version   "1.00"
#property description "CopierR low-latency MT5 bridge"

#include <Trade/Trade.mqh>

input string InpEndpoint = "127.0.0.1:48100";
input string InpAccountId = "mt5-account";
input string InpToken = "replace-me";
input bool   InpPublishEvents = true;
input ulong  InpCopyMagic = 28082805;
input int    InpPollMs = 5;
input ulong  InpDeviationPoints = 20;

#import "copierr_bridge.dll"
int  CopierRConnect(string endpoint);
int  CopierRSend(int handle, string line);
int  CopierRPoll(int handle, uchar &buffer[], int capacity);
void CopierRClose(int handle);
#import

CTrade g_trade;
int g_handle = -1;
datetime g_last_connect_attempt = 0;

int OnInit()
{
   g_trade.SetExpertMagicNumber(InpCopyMagic);
   g_trade.SetDeviationInPoints(InpDeviationPoints);
   EventSetMillisecondTimer(MathMax(5, InpPollMs));
   EnsureConnected();
   return(INIT_SUCCEEDED);
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(g_handle >= 0)
   {
      CopierRClose(g_handle);
      g_handle = -1;
   }
}

void OnTick()
{
   PollCommands();
}

void OnTimer()
{
   EnsureConnected();
   PollCommands();
}

void OnTradeTransaction(const MqlTradeTransaction &trans,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
{
   if(!InpPublishEvents || g_handle < 0)
      return;

   if(trans.type == TRADE_TRANSACTION_DEAL_ADD && trans.deal > 0)
   {
      if(!HistoryDealSelect(trans.deal))
         return;

      ulong magic = (ulong)HistoryDealGetInteger(trans.deal, DEAL_MAGIC);
      if(magic == InpCopyMagic)
         return;

      long entry = HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
      long deal_type = HistoryDealGetInteger(trans.deal, DEAL_TYPE);
      ulong position_identifier = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
      string symbol = HistoryDealGetString(trans.deal, DEAL_SYMBOL);
      double deal_volume = HistoryDealGetDouble(trans.deal, DEAL_VOLUME);
      double deal_price = HistoryDealGetDouble(trans.deal, DEAL_PRICE);

      if(entry == DEAL_ENTRY_IN || entry == DEAL_ENTRY_INOUT)
      {
         if(SelectPositionByIdentifier(position_identifier))
         {
            ulong position_magic = (ulong)PositionGetInteger(POSITION_MAGIC);
            if(position_magic == InpCopyMagic)
               return;
            double current_volume = PositionGetDouble(POSITION_VOLUME);
            SendPositionEvent("open", position_identifier, symbol,
                              PositionSide(), current_volume, current_volume,
                              deal_price, PositionGetDouble(POSITION_SL), PositionGetDouble(POSITION_TP));
         }
         return;
      }

      if(entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY)
      {
         if(SelectPositionByIdentifier(position_identifier))
         {
            ulong position_magic = (ulong)PositionGetInteger(POSITION_MAGIC);
            if(position_magic == InpCopyMagic)
               return;
            double remaining = PositionGetDouble(POSITION_VOLUME);
            SendPositionEvent("reduce", position_identifier, symbol,
                              PositionSide(), deal_volume, remaining,
                              deal_price, PositionGetDouble(POSITION_SL), PositionGetDouble(POSITION_TP));
         }
         else
         {
            string original_side = deal_type == DEAL_TYPE_SELL ? "buy" : "sell";
            SendPositionEvent("close", position_identifier, symbol,
                              original_side, deal_volume, 0.0,
                              deal_price, 0.0, 0.0);
         }
         return;
      }
   }

   if(trans.type == TRADE_TRANSACTION_POSITION && trans.position > 0)
   {
      if(!PositionSelectByTicket(trans.position))
         return;
      ulong position_magic = (ulong)PositionGetInteger(POSITION_MAGIC);
      if(position_magic == InpCopyMagic)
         return;
      ulong identifier = (ulong)PositionGetInteger(POSITION_IDENTIFIER);
      double volume = PositionGetDouble(POSITION_VOLUME);
      SendPositionEvent("modify", identifier, PositionGetString(POSITION_SYMBOL),
                        PositionSide(), volume, volume,
                        PositionGetDouble(POSITION_PRICE_OPEN), PositionGetDouble(POSITION_SL), PositionGetDouble(POSITION_TP));
   }
}

bool EnsureConnected()
{
   if(g_handle >= 0)
      return(true);

   datetime now = TimeLocal();
   if(now == g_last_connect_attempt)
      return(false);
   g_last_connect_attempt = now;

   int handle = CopierRConnect(InpEndpoint);
   if(handle < 0)
      return(false);

   g_handle = handle;
   string hello = "HELLO\t1\t" + CleanField(InpAccountId) + "\tmt5\t" + CleanField(InpToken);
   if(CopierRSend(g_handle, hello) < 0)
   {
      DisconnectBridge();
      return(false);
   }
   return(true);
}

void DisconnectBridge()
{
   if(g_handle >= 0)
      CopierRClose(g_handle);
   g_handle = -1;
}

void PollCommands()
{
   if(g_handle < 0)
      return;

   uchar buffer[4096];
   for(int i = 0; i < 128; i++)
   {
      ArrayInitialize(buffer, 0);
      int count = CopierRPoll(g_handle, buffer, ArraySize(buffer));
      if(count == 0)
         return;
      if(count < 0)
      {
         DisconnectBridge();
         return;
      }
      string line = CharArrayToString(buffer, 0, count, CP_UTF8);
      StringReplace(line, "\r", "");
      StringReplace(line, "\n", "");
      if(StringLen(line) > 0)
         HandleCommand(line);
   }
}

void HandleCommand(string line)
{
   string parts[];
   ushort separator = StringGetCharacter("\t", 0);
   int count = StringSplit(line, separator, parts);
   if(count != 18 || parts[0] != "COMMAND" || parts[1] != "1")
      return;
   if(parts[7] != InpAccountId)
      return;

   string command_id = parts[2];
   string action = parts[8];
   ulong target_ticket = (ulong)StringToInteger(parts[9]);
   string symbol = parts[10];
   string side = parts[11];
   double volume = NormalizeLots(symbol, StringToDouble(parts[12]));
   double sl = StringToDouble(parts[16]);
   double tp = StringToDouble(parts[17]);

   bool ok = false;
   ulong external_ticket = target_ticket;
   string message = "";

   ResetLastError();
   g_trade.SetExpertMagicNumber(InpCopyMagic);
   g_trade.SetDeviationInPoints(InpDeviationPoints);

   if(action == "open")
   {
      if(!SymbolSelect(symbol, true))
      {
         SendAck(command_id, "rejected", "", "SymbolSelect failed");
         return;
      }

      string comment = "CopierR:" + command_id;
      if(side == "buy")
         ok = g_trade.Buy(volume, symbol, 0.0, sl, tp, comment);
      else
         ok = g_trade.Sell(volume, symbol, 0.0, sl, tp, comment);

      if(!ok && (sl > 0.0 || tp > 0.0))
      {
         ResetLastError();
         if(side == "buy")
            ok = g_trade.Buy(volume, symbol, 0.0, 0.0, 0.0, comment);
         else
            ok = g_trade.Sell(volume, symbol, 0.0, 0.0, 0.0, comment);
      }

      if(ok)
      {
         ulong deal = g_trade.ResultDeal();
         ulong position_identifier = 0;
         if(deal > 0 && HistoryDealSelect(deal))
            position_identifier = (ulong)HistoryDealGetInteger(deal, DEAL_POSITION_ID);
         external_ticket = FindPositionTicketByIdentifier(position_identifier);
         if(external_ticket == 0)
         {
            SendAck(command_id, "unknown", "", "order accepted but position ticket not resolved");
            return;
         }
         if((sl > 0.0 || tp > 0.0) && PositionSelectByTicket(external_ticket))
            g_trade.PositionModify(external_ticket, sl, tp);
      }
      else
      {
         message = g_trade.ResultRetcodeDescription();
      }
   }
   else if(action == "modify")
   {
      ok = g_trade.PositionModify(target_ticket, sl, tp);
      if(!ok)
         message = g_trade.ResultRetcodeDescription();
   }
   else if(action == "reduce")
   {
      ok = ReducePosition(target_ticket, volume, command_id);
      if(!ok)
         message = g_trade.ResultRetcodeDescription();
   }
   else if(action == "close")
   {
      ok = g_trade.PositionClose(target_ticket);
      if(!ok)
         message = g_trade.ResultRetcodeDescription();
   }

   SendAck(command_id, ok ? "filled" : "rejected",
           ok ? LongToString((long)external_ticket) : "", message);
}

bool ReducePosition(ulong ticket, double volume, string command_id)
{
   if(!PositionSelectByTicket(ticket))
      return(false);

   ENUM_ACCOUNT_MARGIN_MODE margin_mode = (ENUM_ACCOUNT_MARGIN_MODE)AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   if(margin_mode == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING)
      return(g_trade.PositionClosePartial(ticket, volume));

   string symbol = PositionGetString(POSITION_SYMBOL);
   ENUM_POSITION_TYPE position_type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   string comment = "CopierR:" + command_id;
   if(position_type == POSITION_TYPE_BUY)
      return(g_trade.Sell(volume, symbol, 0.0, 0.0, 0.0, comment));
   return(g_trade.Buy(volume, symbol, 0.0, 0.0, 0.0, comment));
}

bool SelectPositionByIdentifier(ulong identifier)
{
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0)
         continue;
      if((ulong)PositionGetInteger(POSITION_IDENTIFIER) == identifier)
         return(true);
   }
   return(false);
}

ulong FindPositionTicketByIdentifier(ulong identifier)
{
   if(identifier == 0)
      return(0);
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0)
         continue;
      if((ulong)PositionGetInteger(POSITION_IDENTIFIER) == identifier)
         return(ticket);
   }
   return(0);
}

string PositionSide()
{
   ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
   return(type == POSITION_TYPE_BUY ? "buy" : "sell");
}

void SendPositionEvent(string action, ulong source_order_id, string symbol,
                       string side, double event_volume, double remaining_volume,
                       double price, double sl, double tp)
{
   string event_id = StringFormat("%s:%I64u:%s:%.8f:%.8f:%.8f:%.8f",
                                  InpAccountId, source_order_id, action,
                                  event_volume, remaining_volume, sl, tp);
   string line = "EVENT\t1\t" + CleanField(event_id) + "\t" + CleanField(InpAccountId) +
                 "\tmt5\t" + action + "\t" + LongToString((long)source_order_id) + "\t" +
                 CleanField(symbol) + "\t" + side + "\t" + DoubleToString(event_volume, 8) +
                 "\t" + DoubleToString(remaining_volume, 8) + "\t" + DoubleToString(price, 8) +
                 "\t" + OptionalPrice(sl) + "\t" + OptionalPrice(tp) + "\t" +
                 LongToString(NowNs()) + "\t";
   if(CopierRSend(g_handle, line) < 0)
      DisconnectBridge();
}

void SendAck(string command_id, string status, string external_id, string message)
{
   if(g_handle < 0)
      return;
   string line = "ACK\t1\t" + CleanField(command_id) + "\t" + CleanField(InpAccountId) +
                 "\t" + status + "\t" + CleanField(external_id) + "\t" + LongToString(NowNs()) +
                 "\t" + CleanField(message);
   if(CopierRSend(g_handle, line) < 0)
      DisconnectBridge();
}

double NormalizeLots(string symbol, double lots)
{
   double minimum = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maximum = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0.0)
      step = 0.01;
   lots = MathMax(minimum, MathMin(maximum, lots));
   return(MathRound(lots / step) * step);
}

string OptionalPrice(double value)
{
   if(value <= 0.0)
      return("");
   return(DoubleToString(value, 8));
}

string CleanField(string value)
{
   StringReplace(value, "\t", " ");
   StringReplace(value, "\r", " ");
   StringReplace(value, "\n", " ");
   return(value);
}

long NowNs()
{
   return((long)TimeGMT() * 1000000000);
}
