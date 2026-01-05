from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from shipment_qna_bot.graph.nodes.analytics_planner import \
    analytics_planner_node
from shipment_qna_bot.graph.nodes.answer import answer_node
from shipment_qna_bot.graph.nodes.extractor import extractor_node
from shipment_qna_bot.graph.nodes.intent import intent_node
from shipment_qna_bot.graph.nodes.judge import judge_node
from shipment_qna_bot.graph.nodes.normalizer import normalize_node
from shipment_qna_bot.graph.nodes.planner import planner_node
from shipment_qna_bot.graph.nodes.retrieve import retrieve_node
from shipment_qna_bot.graph.nodes.router import route_node
from shipment_qna_bot.graph.state import GraphState


def should_continue(state: GraphState):
    """
    Conditional edge to determine if we should retry retrieval or finish.
    """
    if state.get("is_satisfied"):
        return "end"
    if state.get("retry_count", 0) >= state.get("max_retries", 3):
        return "end"

    intent = state.get("intent")
    if intent == "analytics":
        return "retry_analytics"
    return "retry_retrieval"


def build_graph():
    """
    Constructs the shipment QnA graph.
    """
    workflow = StateGraph(GraphState)

    # --- Add Nodes ---
    workflow.add_node("normalizer", normalize_node)
    workflow.add_node("extractor", extractor_node)
    workflow.add_node("intent", intent_node)
    workflow.add_node("planner", planner_node)
    workflow.add_node("analytics_planner", analytics_planner_node)
    workflow.add_node("retrieve", retrieve_node)
    workflow.add_node("answer", answer_node)
    workflow.add_node("judge", judge_node)

    # --- Add Edges ---
    # Start -> Normalizer
    workflow.set_entry_point("normalizer")

    # Normalizer -> Extractor
    workflow.add_edge("normalizer", "extractor")

    # Extractor -> Intent
    workflow.add_edge("extractor", "intent")

    # Intent -> Router (Conditional)
    workflow.add_conditional_edges(
        "intent",
        route_node,
        {
            "retrieval": "planner",
            "analytics": "analytics_planner",
            "end": END,
        },
    )

    # Retrieval Flow
    workflow.add_edge("planner", "retrieve")
    workflow.add_edge("analytics_planner", "retrieve")
    workflow.add_edge("retrieve", "answer")
    workflow.add_edge("answer", "judge")

    # Reflective Loop
    workflow.add_conditional_edges(
        "judge",
        should_continue,
        {
            "retry_retrieval": "planner",
            "retry_analytics": "analytics_planner",
            "end": END,
        },
    )

    # --- Checkpointer ---
    # Using MemorySaver for in-memory durable execution (Session scope)
    checkpointer = MemorySaver()

    return workflow.compile(checkpointer=checkpointer)


# Singleton instance
graph_app = build_graph()


def run_graph(input_state: dict) -> dict:
    """
    Synchronous wrapper to run the graph.
    """
    thread_id = input_state.get("conversation_id", "default")
    config = {"configurable": {"thread_id": thread_id}}

    # Initialize control flow fields if not present
    if "retry_count" not in input_state:
        input_state["retry_count"] = 0
    if "max_retries" not in input_state:
        input_state["max_retries"] = 3
    if "is_satisfied" not in input_state:
        input_state["is_satisfied"] = False
    if "usage_metadata" not in input_state:
        input_state["usage_metadata"] = {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }

    # Convert question_raw to a message for history persistence
    from langchain_core.messages import HumanMessage

    # We always append the current question to the message history if it's a new turn.
    # In LangGraph, if we use add_messages, we just provide the new message.
    input_state["messages"] = [HumanMessage(content=input_state["question_raw"])]

    return graph_app.invoke(input_state, config=config)
