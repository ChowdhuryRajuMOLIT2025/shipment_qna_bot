from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from shipment_qna_bot.graph.nodes.analytics_planner import \
    analytics_planner_node
from shipment_qna_bot.graph.nodes.answer import answer_node
from shipment_qna_bot.graph.nodes.extractor import extractor_node
from shipment_qna_bot.graph.nodes.intent import intent_node
from shipment_qna_bot.graph.nodes.normalizer import normalize_node
from shipment_qna_bot.graph.nodes.planner import planner_node
from shipment_qna_bot.graph.nodes.retrieve import retrieve_node
from shipment_qna_bot.graph.nodes.router import route_node
from shipment_qna_bot.graph.state import GraphState


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
    workflow.add_edge("answer", END)

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

    # Convert question_raw to a message for history persistence
    from langchain_core.messages import HumanMessage

    if "messages" not in input_state:
        input_state["messages"] = [HumanMessage(content=input_state["question_raw"])]

    return graph_app.invoke(input_state, config=config)
