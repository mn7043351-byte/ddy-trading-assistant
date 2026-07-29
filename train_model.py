import asyncio
import websockets
import json
import random
import time
from datetime import datetime

# Simulated ML Strategy parameters for CORE_01 and CORE_02
CORE_01_PARAMS = {"fast_ema": 9, "slow_ema": 21, "rsi_period": 14}
CORE_02_PARAMS = {"learning_rate": 0.01, "epochs": 100, "momentum": 0.9}

def generate_log_msg(msg):
    return {
        "type": "training_log",
        "msg": f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    }

def generate_stats_update(core_id, total, correct):
    return {
        "type": "training_stats",
        "core": core_id,
        "data": {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "prediction": "BUY" if random.random() > 0.5 else "SELL",
            "actual": "BUY", # Simulated
            "accuracy": 100 if random.random() < (correct/total if total > 0 else 0.5) else 0
        }
    }

async def ai_training_loop():
    uri = "ws://localhost:8765"
    
    while True:
        try:
            print(f"Connecting to DDY Server Engine at {uri}...")
            async with websockets.connect(uri) as websocket:
                print("Connected! Starting Deep Learning Optimization Matrix...")
                
                # Initial greeting
                await websocket.send(json.dumps(generate_log_msg("[AI_CORE] Deep Learning Matrix Connected.")))
                await asyncio.sleep(1)
                await websocket.send(json.dumps(generate_log_msg("[AI_CORE] Synchronizing historical data weights...")))
                
                evals_c1 = 0
                correct_c1 = 0
                evals_c2 = 0
                correct_c2 = 0

                while True:
                    await asyncio.sleep(random.randint(3, 7)) # Simulate processing time

                    # Choose random core to train
                    if random.random() > 0.5:
                        # Train CORE 01
                        evals_c1 += 1
                        success = random.random() > 0.3 # 70% win rate simulation
                        if success: correct_c1 += 1
                        
                        fast = random.randint(5, 12)
                        slow = random.randint(20, 30)
                        
                        log_msg = f"[CORE_01] Tested EMA({fast},{slow}). Win Rate: {round(correct_c1/evals_c1*100)}%."
                        await websocket.send(json.dumps(generate_log_msg(log_msg)))
                        await websocket.send(json.dumps(generate_stats_update("core1", evals_c1, correct_c1)))
                    else:
                        # Train CORE 02
                        evals_c2 += 1
                        success = random.random() > 0.15 # 85% win rate simulation for ML
                        if success: correct_c2 += 1
                        
                        lr = round(random.uniform(0.001, 0.05), 4)
                        
                        log_msg = f"[CORE_02] Epoch {evals_c2} completed. LR: {lr}. Loss decreasing."
                        await websocket.send(json.dumps(generate_log_msg(log_msg)))
                        await websocket.send(json.dumps(generate_stats_update("core2", evals_c2, correct_c2)))
                        
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError):
            print("Connection failed or server offline. Retrying in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    print("=======================================")
    print(" DDY.AI - Neural Training Core Agent   ")
    print("=======================================")
    asyncio.run(ai_training_loop())
