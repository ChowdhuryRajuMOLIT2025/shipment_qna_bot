from shipment_qna_bot.graph.nodes.intent import intent_node
from shipment_qna_bot.graph.nodes.router import route_node


def test_weather_queries_do_not_fall_back_to_plain_analytics():
    state = {
        "normalized_question": "show me a chart of weather impact by discharge port",
        "question_raw": "show me a chart of weather impact by discharge port",
        "messages": [],
    }

    result = intent_node(state)

    assert result["intent"] == "retrieval"
    assert "weather" in result["sub_intents"]
    assert route_node(result) == "weather_impact"
