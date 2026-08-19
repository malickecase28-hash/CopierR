#property strict
#property version   "1.00"
#property description "CopierR low-latency MT4 bridge"

input string InpEndpoint = "127.0.0.1:48100";
input string InpAccountId = "mt4-account";
input string InpToken = "replace-me";
input bool   InpPublishEvents = true;
input bool   InpCopyExistingOnStart = false;
input int    InpCopyMagic = 28082804;
input int    InpPollMs = 10;
input int    InpSlippage = 5;

#import "copierr_bridge.dll"
int  CopierRConnect(string endpoint);
int  CopierRSend(int handle, string line);
int  CopierRPoll(int handle, uchar &buffer[], int capacity);
void CopierRClose(int handle);
#import

struct OrderSnapshot
{
   int ticket;
   string symbol;
   int type;
   double lots;
   double open_price;
   double sl;
   double tp;
};

int g_handle = -1;
OrderSnapshot g_previous[];
bool g_snapshot_ready = false;
datetime g_last_connect_attempt = 0;

int OnInit()
{
   EventSetMillisecondTimer(MathMax(10, InpPollMs));
   EnsureConnected();
   SyncOrders();
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
   SyncOrders();
   PollCommands();
}

void OnTimer()
{
   EnsureConnected();
   PollCommands();
   SyncOrders();
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
   string hello = "HELLO\t1\t" + CleanField(InpAccountId) + "\tmt4\t" + CleanField(InpToken);
   if(CopierRSend(g_handle, hello) < 0)
   {
      CopierRClose(g_handle);
      g_handle = -1;
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
   for(int i = 0; i < 64; i++)
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
   int target_ticket = (int)StringToInteger(parts[9]);
   string symbol = parts[10];
   string side = parts[11];
   double volume = NormalizeLots(symbol, StringToDouble(parts[12]));
   double sl = StringToDouble(parts[16]);
   double tp = StringToDouble(parts[17]);

   bool ok = false;
   int external_ticket = target_ticket;
   string message = "";

   ResetLastError();
   if(action == "open")
   {
      int order_type = side == "buy" ? OP_BUY : OP_SELL;
      double price = order_type == OP_BUY ? MarketInfo(symbol, MODE_ASK) : MarketInfo(symbol, MODE_BID);
      int ticket = OrderSend(symbol, order_type, volume, price, InpSlippage, sl, tp,
                             "CopierR:" + command_id, InpCopyMagic, 0, clrNONE);
      if(ticket < 0 && (sl > 0.0 || tp > 0.0))
      {
         ResetLastError();
         ticket = OrderSend(symbol, order_type, volume, price, InpSlippage, 0, 0,
                            "CopierR:" + command_id, InpCopyMagic, 0, clrNONE);
         if(ticket >= 0 && OrderSelect(ticket, SELECT_BY_TICKET, MODE_TRADES))
            OrderModify(ticket, OrderOpenPrice(), sl, tp, 0, clrNONE);
      }
      if(ticket >= 0)
      {
         ok = true;
         external_ticket = ticket;
      }
      else
      {
         message = "OrderSend error " + IntegerToString(GetLastError());
      }
   }
   else if(action == "modify")
   {
      if(OrderSelect(target_ticket, SELECT_BY_TICKET, MODE_TRADES))
         ok = OrderModify(target_ticket, OrderOpenPrice(), sl, tp, 0, clrNONE);
      if(!ok)
         message = "OrderModify error " + IntegerToString(GetLastError());
   }
   else if(action == "reduce" || action == "close")
   {
      if(OrderSelect(target_ticket, SELECT_BY_TICKET, MODE_TRADES))
      {
         double close_lots = action == "close" ? OrderLots() : NormalizeLots(OrderSymbol(), volume);
         double price = OrderType() == OP_BUY ? MarketInfo(OrderSymbol(), MODE_BID) : MarketInfo(OrderSymbol(), MODE_ASK);
         ok = OrderClose(target_ticket, close_lots, price, InpSlippage, clrNONE);
      }
      if(!ok)
         message = "OrderClose error " + IntegerToString(GetLastError());
   }

   SendAck(command_id, ok ? "filled" : "rejected",
           ok ? IntegerToString(external_ticket) : "", message);
}

void SyncOrders()
{
   OrderSnapshot current[];
   CollectOrders(current);

   if(!g_snapshot_ready)
   {
      CopySnapshots(current, g_previous);
      g_snapshot_ready = true;
      if(InpCopyExistingOnStart && InpPublishEvents)
      {
         for(int i = 0; i < ArraySize(current); i++)
            SendOrderEvent("open", current[i], current[i].lots, current[i].lots);
      }
      return;
   }

   if(InpPublishEvents && g_handle >= 0)
   {
      for(int i = 0; i < ArraySize(current); i++)
      {
         int previous_index = FindSnapshot(g_previous, current[i].ticket);
         if(previous_index < 0)
         {
            SendOrderEvent("open", current[i], current[i].lots, current[i].lots);
            continue;
         }

         double lot_step = MarketInfo(current[i].symbol, MODE_LOTSTEP);
         if(lot_step <= 0.0)
            lot_step = 0.01;
         if(current[i].lots < g_previous[previous_index].lots - lot_step / 2.0)
         {
            double reduced = g_previous[previous_index].lots - current[i].lots;
            SendOrderEvent("reduce", current[i], reduced, current[i].lots);
         }

         if(MathAbs(current[i].sl - g_previous[previous_index].sl) > Point / 2.0 ||
            MathAbs(current[i].tp - g_previous[previous_index].tp) > Point / 2.0)
         {
            SendOrderEvent("modify", current[i], current[i].lots, current[i].lots);
         }
      }

      for(int j = 0; j < ArraySize(g_previous); j++)
      {
         if(FindSnapshot(current, g_previous[j].ticket) < 0)
            SendOrderEvent("close", g_previous[j], g_previous[j].lots, 0.0);
      }
   }

   CopySnapshots(current, g_previous);
}

void CollectOrders(OrderSnapshot &out[])
{
   ArrayResize(out, 0);
   for(int i = 0; i < OrdersTotal(); i++)
   {
      if(!OrderSelect(i, SELECT_BY_POS, MODE_TRADES))
         continue;
      if(OrderType() != OP_BUY && OrderType() != OP_SELL)
         continue;
      if(OrderMagicNumber() == InpCopyMagic)
         continue;

      int n = ArraySize(out);
      ArrayResize(out, n + 1);
      out[n].ticket = OrderTicket();
      out[n].symbol = OrderSymbol();
      out[n].type = OrderType();
      out[n].lots = OrderLots();
      out[n].open_price = OrderOpenPrice();
      out[n].sl = OrderStopLoss();
      out[n].tp = OrderTakeProfit();
   }
}

int FindSnapshot(OrderSnapshot &items[], int ticket)
{
   for(int i = 0; i < ArraySize(items); i++)
      if(items[i].ticket == ticket)
         return(i);
   return(-1);
}

void CopySnapshots(OrderSnapshot &source[], OrderSnapshot &destination[])
{
   int count = ArraySize(source);
   ArrayResize(destination, count);
   for(int i = 0; i < count; i++)
      destination[i] = source[i];
}

void SendOrderEvent(string action, OrderSnapshot &order, double event_volume, double remaining_volume)
{
   string side = order.type == OP_BUY ? "buy" : "sell";
   string event_id = StringFormat("%s:%d:%s:%.8f:%.8f:%.8f:%.8f",
                                  InpAccountId, order.ticket, action,
                                  event_volume, remaining_volume, order.sl, order.tp);
   string line = "EVENT\t1\t" + CleanField(event_id) + "\t" + CleanField(InpAccountId) +
                 "\tmt4\t" + action + "\t" + IntegerToString(order.ticket) + "\t" +
                 CleanField(order.symbol) + "\t" + side + "\t" + DoubleToString(event_volume, 8) +
                 "\t" + DoubleToString(remaining_volume, 8) + "\t" + DoubleToString(order.open_price, Digits) +
                 "\t" + OptionalPrice(order.sl) + "\t" + OptionalPrice(order.tp) + "\t" +
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
   double minimum = MarketInfo(symbol, MODE_MINLOT);
   double maximum = MarketInfo(symbol, MODE_MAXLOT);
   double step = MarketInfo(symbol, MODE_LOTSTEP);
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
