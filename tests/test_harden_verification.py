import pandas as pd
import pytest
from fastapi.testclient import TestClient

from shipment_qna_bot.api.main import app
from shipment_qna_bot.graph.nodes.intent import intent_node
from shipment_qna_bot.graph.nodes.normalizer import normalize_node
from shipment_qna_bot.tools.pandas_engine import PandasAnalyticsEngine

client = TestClient(app)


def test_security_headers():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert "Content-Security-Policy" in response.headers
    assert "Strict-Transport-Security" in response.headers


def test_pandas_engine_rce_blocking():
    engine = PandasAnalyticsEngine()
    df = pd.DataFrame({"a": [1]})

    # Attempt 1: __import__
    res1 = engine.execute_code(df, "res = __import__('os').system('ls')")
    assert res1["success"] is False
    assert "forbidden" in res1["error"].lower() or "prohibited" in res1["error"].lower()

    # Attempt 2: Dunder abuse
    res2 = engine.execute_code(df, "res = df.__class__.__base__")
    assert res2["success"] is False
    assert "forbidden" in res2["error"].lower()

    # Attempt 3: Forbidden builtins
    res3 = engine.execute_code(df, "res = open('/etc/passwd').read()")
    assert res3["success"] is False
    assert "forbidden" in res3["error"].lower()


def test_praise_guardrail_intent():
    # Test Normalizer bypass
    state = {"question_raw": "Great job, bot!", "messages": []}
    res_norm = normalize_node(state)
    assert res_norm["normalized_question"] == "great job, bot!"

    # Test Intent classification (Mocking LLM in code isn't easy here, but we can test the test_mode logic)
    import os

    os.environ["SHIPMENT_QNA_BOT_TEST_MODE"] = "true"
    state_intent = {
        "normalized_question": "exactly what i wanted, thanks!",
        "question_raw": "exactly what i wanted, thanks!",
    }
    res_intent = intent_node(state_intent)
    # In test mode, we fixed the exit_words, so 'thanks' shouldn't kill it anymore if it's not a standalone farewell
    # But wait, our test mode logic still has 'thank you' if we didn't update it carefully.
    # Let's check intent.py test mode logic.
    assert res_intent["intent"] != "end"
