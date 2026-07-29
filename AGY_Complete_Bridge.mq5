//+------------------------------------------------------------------+
//|                                           AGY_Complete_Bridge.mq5|
//|                         AGY QUANT Algorithmic Trading Bridge v3.0|
//|                  Full OHLCV Candles, Live Positions & Trade Exec |
//+------------------------------------------------------------------+
#property copyright "Google Antigravity & AGY Quant"
#property link      "http://127.0.0.1:5000"
#property version   "3.00"
#property strict

//--- Input Parameters
input string   PythonServerURL = "http://127.0.0.1:5000/data"; // Flask Backend Endpoint
input int      TimerSeconds    = 1;                            // Polling Frequency (seconds)
input int      CandlesToSend   = 100;                          // Number of historical candles per TF
input double   DefaultLotSize  = 0.01;                         // Fallback Lot Size

//--- Monitored Symbols and Timeframes
string symbols[] = {"BTCUSD", "EURUSD", "GBPUSD", "XAUUSD"};
ENUM_TIMEFRAMES timeframes[] = {PERIOD_M1, PERIOD_M5, PERIOD_M15, PERIOD_H1};

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
  {
   EventSetTimer(TimerSeconds);
   Print("=========================================================");
   Print(" ⚡ AGY QUANT MT5 COMPLETE BRIDGE STARTED ⚡ ");
   Print(" Server URL: ", PythonServerURL);
   Print(" Sending OHLCV candles and listening for orders...");
   Print("=========================================================");
   return(INIT_SUCCEEDED);
  }

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
  {
   EventKillTimer();
   Print("AGY QUANT Bridge Stopped.");
  }

//+------------------------------------------------------------------+
//| Helper: Extract JSON String Value                                |
//+------------------------------------------------------------------+
string ExtractJsonString(string json, string key)
  {
   string search = "\"" + key + "\":\"";
   int pos = StringFind(json, search);
   if(pos < 0) return "";
   pos += StringLen(search);
   int end = StringFind(json, "\"", pos);
   if(end < 0) return "";
   return StringSubstr(json, pos, end - pos);
  }

//+------------------------------------------------------------------+
//| Helper: Extract JSON Number Value                                |
//+------------------------------------------------------------------+
double ExtractJsonDouble(string json, string key)
  {
   string search = "\"" + key + "\":";
   int pos = StringFind(json, search);
   if(pos < 0) return 0.0;
   pos += StringLen(search);
   string num_str = "";
   while(pos < StringLen(json))
     {
      int ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-')
        {
         num_str += StringSubstr(json, pos, 1);
         pos++;
        }
      else break;
     }
   return StringToDouble(num_str);
  }

//+------------------------------------------------------------------+
//| Close an existing position by its MT5 ticket                     |
//+------------------------------------------------------------------+
void ClosePositionByTicket(long ticket)
  {
   if(!PositionSelectByTicket(ticket))
     {
      Print("⚠️ Close requested for ticket #", ticket, " but no matching open position was found (already closed?).");
      return;
     }

   string pos_sym   = PositionGetString(POSITION_SYMBOL);
   double pos_vol   = PositionGetDouble(POSITION_VOLUME);
   long   pos_type  = PositionGetInteger(POSITION_TYPE);

   MqlTradeRequest request;
   MqlTradeResult  result;
   ZeroMemory(request);
   ZeroMemory(result);

   request.action    = TRADE_ACTION_DEAL;
   request.position  = ticket;
   request.symbol    = pos_sym;
   request.volume    = pos_vol;
   request.deviation = 20;
   request.magic     = 888999;
   request.comment   = "AGY Quant UI Close #" + IntegerToString(ticket);
   request.type_time = ORDER_TIME_GTC;

   long filling_mask = SymbolInfoInteger(pos_sym, SYMBOL_FILLING_MODE);
   if((filling_mask & SYMBOL_FILLING_FOK) != 0)
      request.type_filling = ORDER_FILLING_FOK;
   else if((filling_mask & SYMBOL_FILLING_IOC) != 0)
      request.type_filling = ORDER_FILLING_IOC;
   else
      request.type_filling = ORDER_FILLING_RETURN;

   // Closing needs the OPPOSITE order type + matching price
   if(pos_type == POSITION_TYPE_BUY)
     {
      request.type  = ORDER_TYPE_SELL;
      request.price = SymbolInfoDouble(pos_sym, SYMBOL_BID);
     }
   else
     {
      request.type  = ORDER_TYPE_BUY;
      request.price = SymbolInfoDouble(pos_sym, SYMBOL_ASK);
     }

   if(OrderSend(request, result))
     {
      if(result.retcode == TRADE_RETCODE_DONE || result.retcode == TRADE_RETCODE_PLACED)
         Print("✅ Position #", ticket, " closed successfully. Deal #", result.deal);
      else
         Print("⚠️ Close OrderSend returned code: ", result.retcode, " - ", result.comment);
     }
   else
     {
      Print("❌ Close OrderSend Failed! Error Code: ", GetLastError(), " - ", result.comment);
     }
  }

//+------------------------------------------------------------------+
//| Execute Order in MT5 Terminal                                    |
//+------------------------------------------------------------------+
void ExecuteTradeCommand(string json_cmd)
  {
   string action = ExtractJsonString(json_cmd, "action");
   if(action == "" || action == "none") return;

   long ticket_ref = (long)ExtractJsonDouble(json_cmd, "ticket");

   // Handle position close commands from the dashboard separately —
   // these must close an EXISTING position by ticket, not open a new one.
   if(action == "CLOSE")
     {
      Print("📦 Received CLOSE Command from Dashboard for Ticket #", ticket_ref);
      ClosePositionByTicket(ticket_ref);
      return;
     }

   string sym = ExtractJsonString(json_cmd, "symbol");
   if(sym == "") sym = _Symbol;
   
   double vol = ExtractJsonDouble(json_cmd, "volume");
   if(vol <= 0) vol = DefaultLotSize;
   
   double sl = ExtractJsonDouble(json_cmd, "sl");
   double tp = ExtractJsonDouble(json_cmd, "tp");
   
   Print("📦 Received Order Command from Dashboard: ", action, " ", vol, " lots on ", sym, " (Ref #", ticket_ref, ")");
   
   MqlTradeRequest request;
   MqlTradeResult  result;
   ZeroMemory(request);
   ZeroMemory(result);
   
   request.action   = TRADE_ACTION_DEAL;
   request.symbol   = sym;
   request.volume   = vol;
   request.deviation = 20;
   request.magic    = 888999;
   request.comment  = "AGY Quant UI #" + IntegerToString(ticket_ref);
   request.type_time = ORDER_TIME_GTC;

   // Auto-detect broker/symbol supported filling mode (fixes "Unsupported filling mode" / retcode 10030)
   long filling_mask = SymbolInfoInteger(sym, SYMBOL_FILLING_MODE);
   if((filling_mask & SYMBOL_FILLING_FOK) != 0)
      request.type_filling = ORDER_FILLING_FOK;
   else if((filling_mask & SYMBOL_FILLING_IOC) != 0)
      request.type_filling = ORDER_FILLING_IOC;
   else
      request.type_filling = ORDER_FILLING_RETURN;
   
   double price = 0.0;
   if(action == "BUY")
     {
      request.type = ORDER_TYPE_BUY;
      price = SymbolInfoDouble(sym, SYMBOL_ASK);
     }
   else if(action == "SELL")
     {
      request.type = ORDER_TYPE_SELL;
      price = SymbolInfoDouble(sym, SYMBOL_BID);
     }
   else return;
   
   request.price = price;
   if(sl > 0) request.sl = sl;
   if(tp > 0) request.tp = tp;
   
   // Send order to MT5 trade server
   if(OrderSend(request, result))
     {
      if(result.retcode == TRADE_RETCODE_DONE || result.retcode == TRADE_RETCODE_PLACED)
        {
         Print("✅ MT5 Order Executed Successfully! MT5 Deal #", result.deal, ", Order #", result.order);
        }
      else
        {
         Print("⚠️ OrderSend returned code: ", result.retcode, " - ", result.comment);
        }
     }
   else
     {
      Print("❌ OrderSend Failed! Error Code: ", GetLastError(), " - ", result.comment);
     }
  }

//+------------------------------------------------------------------+
//| Timer function: Harvest Data and Poll Server                     |
//+------------------------------------------------------------------+
void OnTimer()
  {
   string json = "[";
   int added_sym = 0;
   
   for(int i=0; i<ArraySize(symbols); i++)
     {
      string sym = symbols[i];
      double bid = SymbolInfoDouble(sym, SYMBOL_BID);
      double ask = SymbolInfoDouble(sym, SYMBOL_ASK);
      if(bid == 0.0 || ask == 0.0) continue;
      
      if(added_sym > 0) json += ",";
      json += "{\"symbol\":\"" + sym + "\",\"bid\":" + DoubleToString(bid, 5) + ",\"ask\":" + DoubleToString(ask, 5) + ",";
      
      // Account Info (send on first symbol)
      if(added_sym == 0)
        {
         json += "\"account\":{\"balance\":" + DoubleToString(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ",";
         json += "\"equity\":" + DoubleToString(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ",";
         json += "\"margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_MARGIN), 2) + ",";
         json += "\"free_margin\":" + DoubleToString(AccountInfoDouble(ACCOUNT_FREEMARGIN), 2) + "},";
        }
      
      // Active Positions
      if(added_sym == 0)
        {
         json += "\"positions\":[";
         int pos_total = PositionsTotal();
         int added_pos = 0;
         for(int p=0; p<pos_total; p++)
           {
            if(PositionGetSymbol(p) != "")
              {
               if(added_pos > 0) json += ",";
               long ticket = PositionGetInteger(POSITION_TICKET);
               string pos_sym = PositionGetString(POSITION_SYMBOL);
               long type_int = PositionGetInteger(POSITION_TYPE);
               string type_str = (type_int == POSITION_TYPE_BUY) ? "BUY" : "SELL";
               double vol = PositionGetDouble(POSITION_VOLUME);
               double open_price = PositionGetDouble(POSITION_PRICE_OPEN);
               double curr_price = PositionGetDouble(POSITION_PRICE_CURRENT);
               double sl_val = PositionGetDouble(POSITION_SL);
               double tp_val = PositionGetDouble(POSITION_TP);
               double profit = PositionGetDouble(POSITION_PROFIT);
               
               json += "{\"ticket\":" + IntegerToString(ticket) + ",";
               json += "\"symbol\":\"" + pos_sym + "\",";
               json += "\"type\":\"" + type_str + "\",";
               json += "\"volume\":" + DoubleToString(vol, 2) + ",";
               json += "\"open_price\":" + DoubleToString(open_price, 5) + ",";
               json += "\"current_price\":" + DoubleToString(curr_price, 5) + ",";
               json += "\"sl\":" + DoubleToString(sl_val, 5) + ",";
               json += "\"tp\":" + DoubleToString(tp_val, 5) + ",";
               json += "\"profit\":" + DoubleToString(profit, 2) + "}";
               added_pos++;
              }
           }
         json += "],";
        }
      
      // Timeframes and OHLCV Candles
      json += "\"timeframes\":[";
      int added_tf = 0;
      for(int j=0; j<ArraySize(timeframes); j++)
        {
         MqlRates rates[];
         ArraySetAsSeries(rates, false); // Oldest first (ascending timestamp)
         int copied = CopyRates(sym, timeframes[j], 0, CandlesToSend, rates);
         
         if(copied > 0)
           {
            if(added_tf > 0) json += ",";
            json += "{\"tf\":\"" + EnumToString(timeframes[j]) + "\",\"candles\":[";
            for(int k=0; k<copied; k++)
              {
               if(k > 0) json += ",";
               json += "{\"time\":" + IntegerToString((long)rates[k].time) + ",";
               json += "\"open\":" + DoubleToString(rates[k].open, 5) + ",";
               json += "\"high\":" + DoubleToString(rates[k].high, 5) + ",";
               json += "\"low\":" + DoubleToString(rates[k].low, 5) + ",";
               json += "\"close\":" + DoubleToString(rates[k].close, 5) + ",";
               json += "\"volume\":" + IntegerToString((long)rates[k].tick_volume) + "}";
              }
            json += "]}";
            added_tf++;
           }
        }
      json += "]}";
      added_sym++;
     }
   json += "]";
   
   // Prepare HTTP POST Request
   char post_data[], result_data[];
   string result_headers;
   StringToCharArray(json, post_data, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(post_data, ArraySize(post_data)-1);
   
   int res = WebRequest("POST", PythonServerURL, "Content-Type: application/json\r\n", 1000, post_data, result_data, result_headers);
   
   if(res == 200)
     {
      string resp = CharArrayToString(result_data, 0, WHOLE_ARRAY, CP_UTF8);
      if(StringFind(resp, "\"action\"") >= 0 && StringFind(resp, "\"none\"") < 0)
        {
         // We received a pending order command from the Python server!
         ExecuteTradeCommand(resp);
        }
     }
   else if(res == -1)
     {
      Print("⚠️ WebRequest Error (Code ", GetLastError(), "). Make sure http://127.0.0.1:5000 is added in MT5 Tools -> Options -> Expert Advisors -> Allow WebRequest!");
     }
  }
//+------------------------------------------------------------------+
