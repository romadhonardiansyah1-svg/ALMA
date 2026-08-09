#property strict
#property version   "1.00"
#property description "ALMA thin MT5 bridge"

#include <Trade/Trade.mqh>

input string BridgeUrl = "http://127.0.0.1:8765";
input string BridgeSecret = "";
input string TerminalId = "mt5-1";
input ulong Magic = 260731;
input int PollMilliseconds = 500;
input int MaxDeviationPoints = 20;

CTrade trade;
string account_mode = "";
string position_mode = "";
long expected_login = 0;
string expected_server = "";
string trade_symbol = "";
ulong sequence = 0;
string session_id = "";
string pending_events[];
string pending_deals[];

string Escape(const string value)
{
   string result = value;
   StringReplace(result, "\\", "\\\\");
   StringReplace(result, "\"", "\\\"");
   StringReplace(result, "\r", "\\r");
   StringReplace(result, "\n", "\\n");
   return result;
}

string IsoTime(const datetime value)
{
   MqlDateTime parts;
   TimeToStruct(value, parts);
   return StringFormat("%04d-%02d-%02dT%02d:%02d:%02dZ",
                       parts.year, parts.mon, parts.day,
                       parts.hour, parts.min, parts.sec);
}

string BoolJson(const bool value) { return value ? "true" : "false"; }
string Num(const double value, const int digits=8)
{
   return DoubleToString(value, digits);
}

string CommentRoot(const string comment)
{
   if(StringFind(comment, "alma-") == 0)
      return comment;
   if(StringFind(comment, "alma:") != 0)
      return "";
   int separator = StringFind(comment, ":", 5);
   if(separator < 0)
      return StringSubstr(comment, 5);
   return StringSubstr(comment, 5, separator - 5);
}

string JsonString(const string json, const string key)
{
   string marker = "\"" + key + "\":\"";
   int start = StringFind(json, marker);
   if(start < 0) return "";
   start += StringLen(marker);
   int finish = StringFind(json, "\"", start);
   if(finish < 0) return "";
   return StringSubstr(json, start, finish - start);
}

double JsonNumber(const string json, const string key)
{
   string marker = "\"" + key + "\":";
   int start = StringFind(json, marker);
   if(start < 0) return 0.0;
   start += StringLen(marker);
   while(start < StringLen(json) && StringGetCharacter(json, start) == ' ') start++;
   bool quoted = start < StringLen(json) && StringGetCharacter(json, start) == '"';
   if(quoted) start++;
   int finish = start;
   while(finish < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, finish);
      if((quoted && ch == '"') || (!quoted && (ch == ',' || ch == '}' || ch == ']')))
         break;
      finish++;
   }
   return StringToDouble(StringSubstr(json, start, finish - start));
}

bool JsonBool(const string json, const string key)
{
   string marker = "\"" + key + "\":";
   int start = StringFind(json, marker);
   if(start < 0) return false;
   start += StringLen(marker);
   return StringSubstr(json, start, 4) == "true";
}

string JsonObject(const string json, const string key)
{
   string marker = "\"" + key + "\":";
   int start = StringFind(json, marker);
   if(start < 0) return "";
   start += StringLen(marker);
   while(start < StringLen(json) && StringGetCharacter(json, start) == ' ') start++;
   if(start >= StringLen(json) || StringGetCharacter(json, start) != '{') return "";
   int depth = 0;
   bool quoted = false;
   for(int i = start; i < StringLen(json); i++)
   {
      ushort ch = StringGetCharacter(json, i);
      if(ch == '"' && (i == 0 || StringGetCharacter(json, i - 1) != '\\')) quoted = !quoted;
      if(quoted) continue;
      if(ch == '{') depth++;
      if(ch == '}' && --depth == 0) return StringSubstr(json, start, i - start + 1);
   }
   return "";
}

string IpcPath(const string name)
{
   return "ALMA\\" + TerminalId + "\\" + name;
}

string ReadIpc(const string name)
{
   int handle = FileOpen(IpcPath(name), FILE_READ | FILE_TXT | FILE_ANSI | FILE_COMMON |
                         FILE_SHARE_READ | FILE_SHARE_WRITE, 0, CP_UTF8);
   if(handle == INVALID_HANDLE) return "";
   string value = FileReadString(handle, (int)FileSize(handle));
   FileClose(handle);
   return value;
}

bool WriteIpc(const string name, const string value)
{
   string temporary = name + ".tmp";
   int handle = FileOpen(IpcPath(temporary), FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_COMMON,
                         0, CP_UTF8);
   if(handle == INVALID_HANDLE) return false;
   FileWriteString(handle, value);
   FileFlush(handle);
   FileClose(handle);
   return FileMove(IpcPath(temporary), FILE_COMMON, IpcPath(name),
                   FILE_COMMON | FILE_REWRITE);
}

string Http(const string method, const string path, const string body, int &status)
{
   status = -1;
   if(method == "GET" && path == "/v1/config")
   {
      string config = ReadIpc("config.json");
      if(config != "") status = 200;
      return config;
   }
   if(method == "POST" && path == "/v1/snapshot")
   {
      if(!WriteIpc("snapshot.json", body)) return "";
      string wanted_session = JsonString(body, "session_id");
      long wanted_sequence = (long)JsonNumber(body, "seq");
      for(int i = 0; i < 250; i++)
      {
         string ack = ReadIpc("snapshot_ack.json");
         if(JsonString(ack, "session_id") == wanted_session &&
            (long)JsonNumber(ack, "seq") == wanted_sequence)
         {
            status = JsonBool(ack, "accepted") ? 200 : 409;
            return ack;
         }
         Sleep(20);
      }
      status = 504;
      return "";
   }
   if(method == "GET" && StringFind(path, "/v1/commands/next") == 0)
   {
      string command = ReadIpc("command.json");
      status = 200;
      return command == "" ? "{\"command\":null}" : "{\"command\":" + command + "}";
   }
   if(method == "POST" && StringFind(path, "/v1/commands/") == 0)
   {
      int start = StringLen("/v1/commands/");
      int finish = StringFind(path, "/ack", start);
      string request_id = finish > start ? StringSubstr(path, start, finish - start) : "";
      if(request_id == "" || StringLen(body) < 2) return "";
      string ack = "{\"request_id\":\"" + Escape(request_id) + "\"," + StringSubstr(body, 1);
      if(!WriteIpc("ack.json", ack)) return "";
      FileDelete(IpcPath("command.json"), FILE_COMMON);
      status = 200;
      return "{\"accepted\":true}";
   }
   return "";
}

string ActualPositionMode()
{
   long mode = AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   if(mode == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) return "HEDGING";
   if(mode == ACCOUNT_MARGIN_MODE_RETAIL_NETTING) return "NETTING";
   return "UNSUPPORTED";
}

string ActualAccountMode()
{
   long mode = AccountInfoInteger(ACCOUNT_TRADE_MODE);
   if(mode == ACCOUNT_TRADE_MODE_DEMO) return "DEMO";
   if(mode != ACCOUNT_TRADE_MODE_REAL) return "UNSUPPORTED";
   string server = AccountInfoString(ACCOUNT_SERVER);
   string marker = server;
   StringToLower(marker);
   if(account_mode == "DEMO" &&
      AccountInfoInteger(ACCOUNT_LOGIN) == expected_login &&
      server == expected_server && StringFind(marker, "trial") >= 0) return "DEMO";
   return "REAL";
}

bool AccountReady()
{
   string actual = ActualPositionMode();
   return actual != "UNSUPPORTED" &&
          (position_mode == "AUTO" || position_mode == actual) &&
          AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) &&
          TerminalInfoInteger(TERMINAL_CONNECTED) &&
          TerminalInfoInteger(TERMINAL_TRADE_ALLOWED);
}

bool NettingSymbolAvailable(const string root, const string symbol)
{
   if(ActualPositionMode() != "NETTING") return true;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || PositionGetString(POSITION_SYMBOL) != symbol) continue;
      if(PositionGetInteger(POSITION_MAGIC) != (long)Magic ||
         CommentRoot(PositionGetString(POSITION_COMMENT)) != root) return false;
   }
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || OrderGetString(ORDER_SYMBOL) != symbol) continue;
      if(OrderGetInteger(ORDER_MAGIC) != (long)Magic ||
         CommentRoot(OrderGetString(ORDER_COMMENT)) != root) return false;
   }
   return true;
}

string PositionsJson()
{
   string result = "[";
   bool first = true;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      string root = CommentRoot(PositionGetString(POSITION_COMMENT));
      long magic = PositionGetInteger(POSITION_MAGIC);
      if(root == "" || magic != (long)Magic) root = "foreign:" + IntegerToString((long)ticket);
      if(!first) result += ",";
      first = false;
      result += StringFormat(
         "{\"ticket\":\"%I64u\",\"root_id\":\"%s\",\"symbol\":\"%s\",\"side\":\"%s\","
         "\"volume\":\"%s\",\"price_open\":\"%s\",\"sl\":\"%s\",\"tp\":\"%s\",\"magic\":%I64u}",
         ticket, Escape(root), Escape(PositionGetString(POSITION_SYMBOL)),
         PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY ? "BUY" : "SELL",
         Num(PositionGetDouble(POSITION_VOLUME)), Num(PositionGetDouble(POSITION_PRICE_OPEN)),
         Num(PositionGetDouble(POSITION_SL)), Num(PositionGetDouble(POSITION_TP)), magic);
   }
   return result + "]";
}

string OrderStatus()
{
   ENUM_ORDER_STATE state = (ENUM_ORDER_STATE)OrderGetInteger(ORDER_STATE);
   if(state == ORDER_STATE_PARTIAL) return "PARTIALLY_FILLED";
   if(state == ORDER_STATE_STARTED || state == ORDER_STATE_PLACED || state == ORDER_STATE_REQUEST_ADD)
      return "ACCEPTED";
   return "SUBMITTED";
}

string OrdersJson()
{
   string result = "[";
   bool first = true;
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0) continue;
      string root = CommentRoot(OrderGetString(ORDER_COMMENT));
      long magic = OrderGetInteger(ORDER_MAGIC);
      if(root == "" || magic != (long)Magic) root = "foreign:" + IntegerToString((long)ticket);
      ENUM_ORDER_TYPE type = (ENUM_ORDER_TYPE)OrderGetInteger(ORDER_TYPE);
      bool buy = type == ORDER_TYPE_BUY || type == ORDER_TYPE_BUY_LIMIT ||
                 type == ORDER_TYPE_BUY_STOP || type == ORDER_TYPE_BUY_STOP_LIMIT;
      if(!first) result += ",";
      first = false;
      result += StringFormat(
         "{\"ticket\":\"%I64u\",\"root_id\":\"%s\",\"symbol\":\"%s\",\"side\":\"%s\","
         "\"order_type\":\"%s\",\"volume\":\"%s\",\"filled_volume\":\"%s\",\"price\":\"%s\","
         "\"status\":\"%s\",\"sl\":\"%s\",\"tp\":\"%s\",\"magic\":%I64u}",
         ticket, Escape(root), Escape(OrderGetString(ORDER_SYMBOL)), buy ? "BUY" : "SELL",
         EnumToString(type), Num(OrderGetDouble(ORDER_VOLUME_INITIAL)),
         Num(OrderGetDouble(ORDER_VOLUME_INITIAL) - OrderGetDouble(ORDER_VOLUME_CURRENT)),
         Num(OrderGetDouble(ORDER_PRICE_OPEN)), OrderStatus(), Num(OrderGetDouble(ORDER_SL)),
         Num(OrderGetDouble(ORDER_TP)), magic);
   }
   return result + "]";
}

string ItemsJson(string &items[])
{
   string result = "[";
   for(int i = 0; i < ArraySize(items); i++)
   {
      if(i > 0) result += ",";
      result += items[i];
   }
   return result + "]";
}

string DealsJson()
{
   if(!HistorySelect(0, TimeCurrent())) return "";
   string result = "[";
   int found = 0;
   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0 || HistoryDealGetInteger(ticket, DEAL_MAGIC) != (long)Magic) continue;
      string root = CommentRoot(HistoryDealGetString(ticket, DEAL_COMMENT));
      if(root == "") continue;
      if(++found > 10000) return "";
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(ticket, DEAL_ENTRY);
      string entry_kind = entry == DEAL_ENTRY_IN ? "IN" : (entry == DEAL_ENTRY_OUT ? "OUT" : "INOUT");
      if(found > 1) result += ",";
      result += StringFormat(
         "{\"deal_id\":\"%I64u\",\"root_id\":\"%s\",\"side\":\"%s\",\"entry_kind\":\"%s\","
         "\"volume\":\"%s\",\"price\":\"%s\",\"fee\":\"%s\",\"timestamp\":\"%s\"}",
         ticket, Escape(root), HistoryDealGetInteger(ticket, DEAL_TYPE) == DEAL_TYPE_BUY ? "BUY" : "SELL",
         entry_kind, Num(HistoryDealGetDouble(ticket, DEAL_VOLUME)),
         Num(HistoryDealGetDouble(ticket, DEAL_PRICE)),
         Num(HistoryDealGetDouble(ticket, DEAL_COMMISSION) + HistoryDealGetDouble(ticket, DEAL_FEE)),
         IsoTime((datetime)HistoryDealGetInteger(ticket, DEAL_TIME)));
   }
   return result + "]";
}

string SnapshotJson()
{
   MqlTick tick;
   if(!SymbolInfoTick(trade_symbol, tick)) return "";
   double margin_buy = 0.0, margin_sell = 0.0;
   double min_volume = SymbolInfoDouble(trade_symbol, SYMBOL_VOLUME_MIN);
   if(!OrderCalcMargin(ORDER_TYPE_BUY, trade_symbol, MathMax(min_volume, 0.01), tick.ask, margin_buy) ||
      !OrderCalcMargin(ORDER_TYPE_SELL, trade_symbol, MathMax(min_volume, 0.01), tick.bid, margin_sell))
      return "";
   double divisor = MathMax(min_volume, 0.01);
   string deals = DealsJson();
   if(deals == "") return "";
   ulong next_sequence = sequence + 1;
   string nonce = session_id + "-" + IntegerToString((long)next_sequence);
   string mode = ActualPositionMode();
   string actual_account_mode = ActualAccountMode();
   return StringFormat(
      "{\"version\":\"alma-mt5-v1\",\"type\":\"snapshot\",\"terminal_id\":\"%s\","
      "\"session_id\":\"%s\",\"seq\":%I64u,\"nonce\":\"%s\",\"timestamp\":\"%s\","
      "\"terminal\":{\"connected\":%s,\"trade_allowed\":%s,\"account_trade_allowed\":%s,"
      "\"account_mode\":\"%s\",\"margin_mode\":\"%s\",\"server\":\"%s\",\"build\":%d},"
      "\"account\":{\"login\":\"%I64d\",\"balance\":\"%s\",\"equity\":\"%s\","
      "\"margin\":\"%s\",\"free_margin\":\"%s\",\"leverage\":%d,\"currency\":\"%s\"},"
      "\"symbol\":{\"name\":\"%s\",\"digits\":%d,\"point\":\"%s\",\"tick_size\":\"%s\","
      "\"tick_value\":\"%s\",\"contract_size\":\"%s\",\"volume_min\":\"%s\","
      "\"volume_max\":\"%s\",\"volume_step\":\"%s\",\"stops_level\":%d,"
      "\"bid\":\"%s\",\"ask\":\"%s\",\"margin_buy_per_lot\":\"%s\","
      "\"margin_sell_per_lot\":\"%s\"},\"positions\":%s,\"orders\":%s,\"events\":%s,\"deals\":%s}",
      Escape(TerminalId), Escape(session_id), next_sequence, Escape(nonce), IsoTime(TimeGMT()),
      BoolJson(TerminalInfoInteger(TERMINAL_CONNECTED)), BoolJson(TerminalInfoInteger(TERMINAL_TRADE_ALLOWED)),
      BoolJson(AccountInfoInteger(ACCOUNT_TRADE_ALLOWED)), actual_account_mode, mode,
      Escape(AccountInfoString(ACCOUNT_SERVER)), (int)TerminalInfoInteger(TERMINAL_BUILD),
      AccountInfoInteger(ACCOUNT_LOGIN), Num(AccountInfoDouble(ACCOUNT_BALANCE), 2),
      Num(AccountInfoDouble(ACCOUNT_EQUITY), 2), Num(AccountInfoDouble(ACCOUNT_MARGIN), 2),
      Num(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2), (int)AccountInfoInteger(ACCOUNT_LEVERAGE),
      Escape(AccountInfoString(ACCOUNT_CURRENCY)), Escape(trade_symbol),
      (int)SymbolInfoInteger(trade_symbol, SYMBOL_DIGITS), Num(SymbolInfoDouble(trade_symbol, SYMBOL_POINT)),
      Num(SymbolInfoDouble(trade_symbol, SYMBOL_TRADE_TICK_SIZE)),
      Num(SymbolInfoDouble(trade_symbol, SYMBOL_TRADE_TICK_VALUE)),
      Num(SymbolInfoDouble(trade_symbol, SYMBOL_TRADE_CONTRACT_SIZE)), Num(min_volume),
      Num(SymbolInfoDouble(trade_symbol, SYMBOL_VOLUME_MAX)), Num(SymbolInfoDouble(trade_symbol, SYMBOL_VOLUME_STEP)),
      (int)SymbolInfoInteger(trade_symbol, SYMBOL_TRADE_STOPS_LEVEL), Num(tick.bid), Num(tick.ask),
      Num(margin_buy / divisor), Num(margin_sell / divisor), PositionsJson(), OrdersJson(),
      ItemsJson(pending_events), deals);
}

bool AlreadyKnown(const string root)
{
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket > 0 && PositionGetInteger(POSITION_MAGIC) == (long)Magic &&
         CommentRoot(PositionGetString(POSITION_COMMENT)) == root) return true;
   }
   for(int i = 0; i < OrdersTotal(); i++)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket > 0 && OrderGetInteger(ORDER_MAGIC) == (long)Magic &&
         CommentRoot(OrderGetString(ORDER_COMMENT)) == root) return true;
   }
   HistorySelect(0, TimeCurrent());
   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket > 0 && HistoryDealGetInteger(ticket, DEAL_MAGIC) == (long)Magic &&
         CommentRoot(HistoryDealGetString(ticket, DEAL_COMMENT)) == root) return true;
   }
   return false;
}

bool TradeSucceeded(const bool invoked)
{
   if(!invoked) return false;
   ulong retcode = trade.ResultRetcode();
   return retcode == TRADE_RETCODE_DONE ||
          retcode == TRADE_RETCODE_PLACED ||
          retcode == TRADE_RETCODE_DONE_PARTIAL;
}

bool PlaceChild(const string root, const string symbol, const bool buy, const double volume,
                const string order_type, const double price, const double trigger,
                const double sl, const double tp, const int index)
{
   if(StringLen(root) > 31) return false;
   string comment = root;
   trade.SetExpertMagicNumber(Magic);
   trade.SetDeviationInPoints(MaxDeviationPoints);
   if(order_type == "LIMIT")
      return TradeSucceeded(
         buy ? trade.BuyLimit(volume, price, symbol, sl, tp, ORDER_TIME_GTC, 0, comment)
             : trade.SellLimit(volume, price, symbol, sl, tp, ORDER_TIME_GTC, 0, comment));
   if(order_type == "STOP_LIMIT")
      return TradeSucceeded(
         trade.OrderOpen(symbol, buy ? ORDER_TYPE_BUY_STOP_LIMIT : ORDER_TYPE_SELL_STOP_LIMIT,
                         volume, price, trigger, sl, tp, ORDER_TIME_GTC, 0, comment));
   if(order_type == "MARKET_PROTECTED")
   {
      MqlTick tick;
      if(!SymbolInfoTick(symbol, tick)) return false;
      if((buy && tick.ask > price) || (!buy && tick.bid < price)) return false;
      return TradeSucceeded(
         buy ? trade.Buy(volume, symbol, 0, sl, tp, comment)
             : trade.Sell(volume, symbol, 0, sl, tp, comment));
   }
   return false;
}

bool PlaceOrder(const string request_id, const string payload)
{
   string marker = "ALMA." + request_id;
   if(GlobalVariableCheck(marker)) return AlreadyKnown(request_id);
   if(!AccountReady()) return false;
   string symbol = JsonString(payload, "symbol");
   string side = JsonString(payload, "side");
   string order_type = JsonString(payload, "order_type");
   double quantity = JsonNumber(payload, "quantity");
   double price = JsonNumber(payload, "price");
   double trigger = JsonNumber(payload, "trigger_price");
   double sl = JsonNumber(payload, "stop_loss");
   long expires_at = (long)JsonNumber(payload, "expires_at_unix");
   string targets_marker = "\"take_profits\":[";
   int cursor = StringFind(payload, targets_marker);
   if(symbol != trade_symbol || (side != "BUY" && side != "SELL") ||
      quantity <= 0 || sl <= 0 || expires_at <= (long)TimeGMT() || cursor < 0)
      return false;
   if(!NettingSymbolAvailable(request_id, symbol)) return false;
   cursor += StringLen(targets_marker);
   int open = StringFind(payload, "[\"", cursor);
   int middle = StringFind(payload, "\",\"", open + 2);
   int close = StringFind(payload, "\"]", middle + 3);
   if(open < 0 || middle < 0 || close < 0 || StringFind(payload, "[\"", close + 2) >= 0)
      return false;
   double tp = StringToDouble(StringSubstr(payload, open + 2, middle - open - 2));
   double fraction = StringToDouble(StringSubstr(payload, middle + 3, close - middle - 3));
   if(tp <= 0 || MathAbs(fraction - 1.0) > 0.00000001) return false;
   if(!GlobalVariableSet(marker, 0.0)) return false;
   GlobalVariablesFlush();
   bool complete = PlaceChild(request_id, symbol, side == "BUY", quantity, order_type,
                              price, trigger, sl, tp, 0);
   if(complete)
   {
      GlobalVariableSet(marker, 1.0);
      GlobalVariablesFlush();
   }
   return complete;
}

bool CancelRoot(const string root)
{
   bool ok = true;
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket > 0 && OrderGetInteger(ORDER_MAGIC) == (long)Magic &&
         CommentRoot(OrderGetString(ORDER_COMMENT)) == root)
         ok = TradeSucceeded(trade.OrderDelete(ticket)) && ok;
   }
   return ok;
}

bool CloseRoot(const string root, const string symbol)
{
   bool ok = true;
   for(int i = OrdersTotal() - 1; i >= 0; i--)
   {
      ulong ticket = OrderGetTicket(i);
      if(ticket == 0 || OrderGetString(ORDER_SYMBOL) != symbol) continue;
      string current = CommentRoot(OrderGetString(ORDER_COMMENT));
      if(OrderGetInteger(ORDER_MAGIC) != (long)Magic || current == "" ||
         (root != "*" && current != root)) continue;
      ok = TradeSucceeded(trade.OrderDelete(ticket)) && ok;
   }
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || PositionGetString(POSITION_SYMBOL) != symbol) continue;
      string current = CommentRoot(PositionGetString(POSITION_COMMENT));
      if(PositionGetInteger(POSITION_MAGIC) != (long)Magic || current == "" ||
         (root != "*" && current != root)) continue;
      ok = TradeSucceeded(trade.PositionClose(ticket, MaxDeviationPoints)) && ok;
   }
   return ok;
}

double OwnedSignedVolume(const string symbol)
{
   double total = 0.0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || PositionGetString(POSITION_SYMBOL) != symbol ||
         PositionGetInteger(POSITION_MAGIC) != (long)Magic) continue;
      double volume = PositionGetDouble(POSITION_VOLUME);
      total += PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY ? volume : -volume;
   }
   return total;
}

bool ReducePosition(const string request_id, const string symbol, const string side,
                    const double requested_volume, const double expected_actual)
{
   if(symbol != trade_symbol || (side != "BUY" && side != "SELL") || requested_volume <= 0) return false;
   double step = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);
   if(step <= 0 || MathAbs(requested_volume / step - MathRound(requested_volume / step)) > 0.00000001)
      return false;
   double actual = OwnedSignedVolume(symbol);
   if(MathAbs(actual - expected_actual) <= step / 2.0) return true;
   if(actual * expected_actual < 0 || MathAbs(actual) < MathAbs(expected_actual) ||
      (side == "SELL" && actual <= 0) || (side == "BUY" && actual >= 0)) return false;
   double remaining = MathAbs(actual) - MathAbs(expected_actual);
   trade.SetExpertMagicNumber(Magic);
   trade.SetDeviationInPoints(MaxDeviationPoints);
   for(int i = PositionsTotal() - 1; i >= 0 && remaining > step / 2.0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0 || PositionGetString(POSITION_SYMBOL) != symbol ||
         PositionGetInteger(POSITION_MAGIC) != (long)Magic) continue;
      ENUM_POSITION_TYPE type = (ENUM_POSITION_TYPE)PositionGetInteger(POSITION_TYPE);
      if((side == "SELL" && type != POSITION_TYPE_BUY) ||
         (side == "BUY" && type != POSITION_TYPE_SELL)) continue;
      double close_volume = MathMin(remaining, PositionGetDouble(POSITION_VOLUME));
      bool invoked;
      if(ActualPositionMode() == "HEDGING")
         invoked = trade.PositionClosePartial(ticket, close_volume, MaxDeviationPoints);
      else
         invoked = side == "SELL"
            ? trade.Sell(close_volume, symbol, 0, 0, 0, request_id)
            : trade.Buy(close_volume, symbol, 0, 0, 0, request_id);
      if(!TradeSucceeded(invoked)) return false;
      remaining -= close_volume;
   }
   return remaining <= step / 2.0;
}

void Ack(const string request_id, const bool accepted)
{
   int status;
   string body = StringFormat("{\"accepted\":%s,\"result\":{\"retcode\":\"%I64u\"}}",
                              BoolJson(accepted), trade.ResultRetcode());
   Http("POST", "/v1/commands/" + request_id + "/ack", body, status);
}

void PollCommand()
{
   int status;
   string response = Http("GET", "/v1/commands/next?terminal_id=" + TerminalId, "", status);
   if(status != 200 || StringFind(response, "\"command\":null") >= 0) return;
   string command = JsonObject(response, "command");
   string request_id = JsonString(command, "request_id");
   string type = JsonString(command, "type");
   string payload = JsonObject(command, "payload");
   bool accepted = false;
   if(type == "place_order") accepted = PlaceOrder(request_id, payload);
   else if(type == "reduce_position") accepted = ReducePosition(
      request_id, JsonString(payload, "symbol"), JsonString(payload, "side"),
      JsonNumber(payload, "quantity"), JsonNumber(payload, "expected_actual"));
   else if(type == "cancel_order") accepted = CancelRoot(JsonString(payload, "root_id"));
   else if(type == "close_position") accepted = CloseRoot(JsonString(payload, "root_id"), JsonString(payload, "symbol"));
   else if(type == "sync_request") accepted = true;
   Ack(request_id, accepted);
}

int OnInit()
{
   int config_status;
   string config = Http("GET", "/v1/config", "", config_status);
   account_mode = JsonString(config, "account_mode");
   position_mode = JsonString(config, "position_mode");
   expected_login = (long)JsonNumber(config, "login");
   expected_server = JsonString(config, "server");
   trade_symbol = JsonString(config, "symbol");
   bool account_mode_valid = account_mode == "DEMO" || account_mode == "REAL";
   bool position_mode_valid = position_mode == "AUTO" || position_mode == "HEDGING" || position_mode == "NETTING";
   bool identity_complete = expected_login > 0 && expected_server != "" && trade_symbol != "";
   bool symbol_selected = identity_complete && SymbolSelect(trade_symbol, true);
   if(config_status != 200 || !account_mode_valid || !position_mode_valid ||
      !identity_complete || !symbol_selected)
   {
      PrintFormat("ALMA bridge config rejected: status=%d account_mode=%d position_mode=%d identity=%d symbol=%d",
                  config_status, account_mode_valid, position_mode_valid, identity_complete, symbol_selected);
      return INIT_FAILED;
   }
   string actual_mode = ActualAccountMode();
   if(actual_mode != account_mode || AccountInfoInteger(ACCOUNT_LOGIN) != expected_login ||
      AccountInfoString(ACCOUNT_SERVER) != expected_server)
   {
      PrintFormat("ALMA bridge identity rejected: account_mode=%d login=%d server=%d",
                  actual_mode == account_mode,
                  AccountInfoInteger(ACCOUNT_LOGIN) == expected_login,
                  AccountInfoString(ACCOUNT_SERVER) == expected_server);
      return INIT_FAILED;
   }
   if(!AccountReady())
   {
      string actual_position_mode = ActualPositionMode();
      PrintFormat("ALMA account readiness rejected: position_mode=%d account_trade=%d connected=%d terminal_trade=%d",
                  actual_position_mode != "UNSUPPORTED" &&
                  (position_mode == "AUTO" || position_mode == actual_position_mode),
                  AccountInfoInteger(ACCOUNT_TRADE_ALLOWED),
                  TerminalInfoInteger(TERMINAL_CONNECTED),
                  TerminalInfoInteger(TERMINAL_TRADE_ALLOWED));
      return INIT_FAILED;
   }
   session_id = IntegerToString((long)AccountInfoInteger(ACCOUNT_LOGIN)) + "-" +
                IntegerToString((long)GetTickCount64());
   trade.SetExpertMagicNumber(Magic);
   EventSetMillisecondTimer(MathMax(PollMilliseconds, 250));
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason) { EventKillTimer(); }

void OnTimer()
{
   string body = SnapshotJson();
   if(body != "")
   {
      int status;
      Http("POST", "/v1/snapshot", body, status);
      if(status == 200)
      {
         sequence++;
         ArrayResize(pending_events, 0);
         ArrayResize(pending_deals, 0);
         PollCommand();
      }
   }
}

void OnTradeTransaction(const MqlTradeTransaction &transaction,
                        const MqlTradeRequest &request,
                        const MqlTradeResult &result)
{
   string root = CommentRoot(request.comment);
   if(root != "")
   {
      string status = result.retcode == TRADE_RETCODE_DONE_PARTIAL ? "PARTIALLY_FILLED" :
                      (result.retcode == TRADE_RETCODE_DONE ||
                       result.retcode == TRADE_RETCODE_PLACED ? "ACCEPTED" : "REJECTED");
      string event = StringFormat(
         "{\"event_id\":\"%I64u-%I64u\",\"request_id\":\"%s\",\"status\":\"%s\","
         "\"ticket\":\"%I64u\",\"volume\":\"%s\",\"filled_volume\":\"%s\","
         "\"price\":\"%s\",\"reason\":\"%s\",\"timestamp\":\"%s\"}",
         GetTickCount64(), result.order, Escape(root), status, result.order,
         Num(request.volume), Num(result.volume), Num(result.price), Escape(result.comment), IsoTime(TimeGMT()));
      int size = ArraySize(pending_events);
      ArrayResize(pending_events, size + 1);
      pending_events[size] = event;
   }
   if(transaction.type == TRADE_TRANSACTION_DEAL_ADD && transaction.deal > 0 &&
      HistoryDealSelect(transaction.deal))
   {
      string deal_root = CommentRoot(HistoryDealGetString(transaction.deal, DEAL_COMMENT));
      if(deal_root == "") return;
      ENUM_DEAL_ENTRY entry = (ENUM_DEAL_ENTRY)HistoryDealGetInteger(transaction.deal, DEAL_ENTRY);
      string entry_kind = entry == DEAL_ENTRY_IN ? "IN" : (entry == DEAL_ENTRY_OUT ? "OUT" : "INOUT");
      string deal = StringFormat(
         "{\"deal_id\":\"%I64u\",\"root_id\":\"%s\",\"side\":\"%s\",\"entry_kind\":\"%s\","
         "\"volume\":\"%s\",\"price\":\"%s\",\"fee\":\"%s\",\"timestamp\":\"%s\"}",
         transaction.deal, Escape(deal_root),
         HistoryDealGetInteger(transaction.deal, DEAL_TYPE) == DEAL_TYPE_BUY ? "BUY" : "SELL",
         entry_kind, Num(HistoryDealGetDouble(transaction.deal, DEAL_VOLUME)),
         Num(HistoryDealGetDouble(transaction.deal, DEAL_PRICE)),
         Num(HistoryDealGetDouble(transaction.deal, DEAL_COMMISSION) + HistoryDealGetDouble(transaction.deal, DEAL_FEE)),
         IsoTime((datetime)HistoryDealGetInteger(transaction.deal, DEAL_TIME)));
      int size = ArraySize(pending_deals);
      ArrayResize(pending_deals, size + 1);
      pending_deals[size] = deal;
   }
}
