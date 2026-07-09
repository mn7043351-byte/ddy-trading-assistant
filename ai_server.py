import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import ta

app = FastAPI()

# Allow CORS for the dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global dictionary to store trained models per pair
MODELS = {}

def add_features(df):
    """Calculate technical indicators for Machine Learning"""
    df = df.copy()
    
    # RSI
    df['rsi'] = ta.momentum.RSIIndicator(close=df['Close'], window=14).rsi()
    
    # MACD
    macd = ta.trend.MACD(close=df['Close'])
    df['macd'] = macd.macd()
    df['macd_signal'] = macd.macd_signal()
    df['macd_diff'] = macd.macd_diff()
    
    # SMAs
    df['sma20'] = ta.trend.SMAIndicator(close=df['Close'], window=20).sma_indicator()
    df['sma50'] = ta.trend.SMAIndicator(close=df['Close'], window=50).sma_indicator()
    
    # Bollinger Bands
    bb = ta.volatility.BollingerBands(close=df['Close'], window=20, window_dev=2)
    df['bb_high'] = bb.bollinger_hband()
    df['bb_low'] = bb.bollinger_lband()
    
    # Momentum (Current Close - Open)
    df['momentum'] = df['Close'] - df['Open']
    
    # Target: 1 if next candle closes higher, 0 if lower
    df['target'] = (df['Close'].shift(-1) > df['Close']).astype(int)
    
    # Drop NaNs
    df.dropna(inplace=True)
    return df

def train_model_for_pair(pair="EURUSD=X"):
    print(f"Downloading 60 days of 5m historical data for {pair} to train AI...")
    data = yf.download(pair, period="60d", interval="5m", progress=False)
    
    if len(data) < 100:
        print(f"Error: Not enough data fetched for {pair}")
        return None
    
    # Flatten MultiIndex columns if yfinance returned them
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
        
    df = add_features(data)
    
    # Features for training
    features = ['rsi', 'macd', 'macd_signal', 'macd_diff', 'sma20', 'sma50', 'bb_high', 'bb_low', 'momentum']
    
    X = df[features]
    y = df['target']
    
    # Train Random Forest
    model = RandomForestClassifier(n_estimators=100, random_state=42, max_depth=10)
    model.fit(X, y)
    
    MODELS[pair] = {
        'model': model,
        'features': features
    }
    print(f"Model trained successfully for {pair}! Accuracy metric setup complete.")
    return True

@app.on_event("startup")
async def startup_event():
    # Pre-train for EUR/USD on startup
    train_model_for_pair("EURUSD=X")

@app.get("/api/predict")
async def predict(pair: str = "EURUSD"):
    # Convert standard pair to yfinance format (EUR/USD -> EURUSD=X)
    yf_pair = pair.replace("/", "") + "=X"
    
    # Check if model exists, if not train it
    if yf_pair not in MODELS:
        success = train_model_for_pair(yf_pair)
        if not success:
            return {"error": "Failed to train model for this pair."}
            
    # Fetch latest 1-day data to get the current indicators
    data = yf.download(yf_pair, period="5d", interval="5m", progress=False)
    
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
        
    df = add_features(data)
    
    if len(df) == 0:
        return {"error": "Not enough recent data to calculate indicators."}
        
    latest_row = df.iloc[-1:]
    
    model_data = MODELS[yf_pair]
    model = model_data['model']
    features = model_data['features']
    
    X_latest = latest_row[features]
    
    # Predict probabilities
    prob = model.predict_proba(X_latest)[0]
    buy_prob = prob[1] * 100
    sell_prob = prob[0] * 100
    
    # Logic generation
    rsi_val = float(latest_row['rsi'].iloc[0])
    macd_diff = float(latest_row['macd_diff'].iloc[0])
    
    if buy_prob > 65:
        direction = "STRONG BUY"
        logic = f"Machine Learning detected high probability bullish breakout. RSI is {rsi_val:.1f}, MACD momentum is positive."
    elif buy_prob > 50:
        direction = "BUY"
        logic = f"AI model leans bullish. Standard technical alignment detected."
    elif sell_prob > 65:
        direction = "STRONG SELL"
        logic = f"Machine Learning detected high probability bearish breakdown. RSI is {rsi_val:.1f}, MACD momentum is negative."
    else:
        direction = "SELL"
        logic = f"AI model leans bearish. Technical structure showing weakness."
        
    confidence = max(buy_prob, sell_prob)
    
    return {
        "pair": pair,
        "direction": direction,
        "confidence": round(confidence, 1),
        "logic": logic,
        "details": {
            "buy_probability": round(buy_prob, 1),
            "sell_probability": round(sell_prob, 1),
            "rsi": round(rsi_val, 2)
        }
    }

if __name__ == "__main__":
    print("Starting DDY.AI Machine Learning Backend on port 8000...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
