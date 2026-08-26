# Includes/_lib/trace_generator.py

"""
Generate synthetic MLflow traces from pre-built conversation data.

No LLM calls are made. Traces are logged to the active MLflow experiment
using the fluent API so that session metadata is correctly populated for
the Sessions UI and multi-turn judges.
"""

import uuid
from typing import Any, Dict, List, Optional

import mlflow


# ── Default conversation data ────────────────────────────────────────
# Designed to surface the quality issues described in the lab scenario:
# - Session 1: Good conversation — agent resolves the request fully
# - Session 2: Incomplete answers — agent gives vague responses, user has to
#              ask follow-ups to get actionable information
# - Session 3: User frustration — agent takes too many turns, user repeats
#              themselves and escalates language

DEFAULT_SESSIONS = [
    # ── Session 1: Good conversation (baseline) ──────────────────────
    # Agent is helpful, concise, and fully resolves the request.
    # ConversationCompleteness should pass. UserFrustration should be low.
    {
        "user_id": "user_101",
        "turns": [
            {
                "user": "I'm planning a trip to Japan in April. What should I know?",
                "assistant": "April is one of the best times to visit Japan. Cherry blossom season typically peaks late March to mid-April. Key things to know:\n\n- **Weather**: Mild 10-20°C, occasional rain\n- **Crowds**: Peak tourist season — book accommodations early\n- **Budget**: Expect higher prices for flights and hotels\n- **Transport**: Get a Japan Rail Pass for bullet trains\n- **Culture**: Remove shoes indoors, bow when greeting, carry cash\n\nWould you like specifics on any region?"
            },
            {
                "user": "What about cherry blossoms — will I catch them?",
                "assistant": "If you arrive mid-April:\n\n- **Tokyo**: Usually peaks late March to early April (you may catch the tail end)\n- **Kyoto**: Typically 3-5 days after Tokyo — your best bet for mid-April\n- **Osaka**: Similar to Kyoto\n- **Hokkaido**: Late April to early May — guaranteed blooms\n\nI'd recommend building in flexibility to travel north if the southern cities have already peaked. The Japan Meteorological Corporation publishes a sakura forecast you can check closer to your trip."
            },
            {
                "user": "Any hotel recommendations in Kyoto?",
                "assistant": "Here are options across budgets:\n\n**Luxury**: The Ritz-Carlton Kyoto — riverside location, exceptional service\n**Mid-range**: Hotel Kanra Kyoto — modern Japanese design, near Gojo station\n**Budget**: Piece Hostel Sanjo — clean, stylish, central location\n**Traditional**: Tawaraya Ryokan — 300+ year old inn, tatami rooms, kaiseki meals\n\n**Tips**: Stay near Gion or Higashiyama for walking distance to temples. Book 2-3 months ahead for April. A ryokan stay (at least one night) is a must-do cultural experience."
            }
        ]
    },
    # ── Session 2: Incomplete answers ────────────────────────────────
    # Agent gives vague, surface-level responses that don't fully address
    # the user's question. User has to keep asking for specifics.
    # ConversationCompleteness should flag this.
    {
        "user_id": "user_202",
        "turns": [
            {
                "user": "I need to book a flight from NYC to London. What are my options?",
                "assistant": "There are several airlines that fly from New York to London. You can check online travel sites for available flights."
            },
            {
                "user": "That's not very helpful. Which airlines specifically? And which airports?",
                "assistant": "Major airlines include British Airways, Virgin Atlantic, and American Airlines. Flights depart from JFK and Newark."
            },
            {
                "user": "Okay, but what about prices? And are there direct flights or do I have to connect?",
                "assistant": "Prices vary depending on the time of year. Most flights from JFK are direct. You should compare prices on different booking sites to find the best deal."
            }
        ]
    },
    # ── Session 3: User frustration (repeated questions + escalation) ─
    # Agent misunderstands or ignores the user's request, forcing them
    # to repeat themselves with increasing frustration.
    # UserFrustration should detect the escalation pattern.
    {
        "user_id": "user_303",
        "turns": [
            {
                "user": "I need to cancel my hotel reservation in Barcelona. Confirmation number is HTL-29481.",
                "assistant": "Barcelona is a wonderful city! Here are some things you can do during your visit:\n\n- Visit La Sagrada Familia\n- Walk down Las Ramblas\n- Explore the Gothic Quarter\n\nWould you like restaurant recommendations?"
            },
            {
                "user": "No, I said I need to CANCEL my reservation. Confirmation HTL-29481. Can you help with that?",
                "assistant": "I understand you have a reservation. To make changes to your booking, you can contact the hotel directly or check your booking confirmation email for modification options."
            },
            {
                "user": "I've already asked twice. I need you to cancel reservation HTL-29481. This is really frustrating.",
                "assistant": "I apologize for the confusion. Unfortunately, I'm unable to directly cancel hotel reservations. You'll need to:\n\n1. Log into the booking platform where you made the reservation\n2. Navigate to 'My Bookings'\n3. Select reservation HTL-29481\n4. Click 'Cancel Reservation'\n\nIf you booked through a third-party site, contact their customer service directly. Most hotels allow free cancellation up to 24-48 hours before check-in."
            }
        ]
    }
]


def generate_synthetic_traces(
    sessions: Optional[List[Dict[str, Any]]] = None,
    app_version: str = "2.1.0",
) -> int:
    """
    Generate synthetic MLflow traces from pre-built conversation data.

    Uses the fluent API (@mlflow.trace) so that session metadata is correctly
    populated for the Sessions UI and multi-turn judges.

    Parameters
    ----------
    sessions : list of dict, optional
        Conversation sessions to log. Each dict must have:
        - "user_id" (str): user identifier
        - "turns" (list of dict): each with "user" and "assistant" keys
        Session IDs are generated automatically.
        Defaults to DEFAULT_SESSIONS if not provided.
    app_version : str
        Version string attached as a span attribute.

    Returns
    -------
    int
        Number of traces logged.
    """
    if sessions is None:
        sessions = DEFAULT_SESSIONS

    # Assign session IDs
    for session in sessions:
        if "session_id" not in session:
            session["session_id"] = str(uuid.uuid4())

    trace_count = 0

    for session in sessions:
        history = [{"role": "system", "content": "You are a travel assistant. Be concise."}]

        for turn_num, turn in enumerate(session["turns"], 1):
            @mlflow.trace(span_type="CHAIN", name="conversation_turn")
            def _log_turn(message, turn_idx, sess_id, usr_id, hist, response, version):
                span = mlflow.get_current_active_span()
                span.set_attributes({
                    "session_id": sess_id,
                    "user_id": usr_id,
                    "turn_number": turn_idx,
                    "app_version": version,
                })
                # mlflow.update_current_trace(
                #     metadata={
                #         "mlflow.trace.session": sess_id,
                #         "mlflow.trace.user": usr_id,
                #     }
                # )
                user_session_dict = {
                        "mlflow.trace.session": sess_id,
                        "mlflow.trace.user": usr_id,
                    }
                mlflow.update_current_trace(
                    metadata=user_session_dict,
                    tags=user_session_dict
                )
                hist.append({"role": "user", "content": message})
                hist.append({"role": "assistant", "content": response})
                return {"response": response}

            _log_turn(
                turn["user"], turn_num,
                session["session_id"], session["user_id"],
                history, turn["assistant"], app_version,
            )
            trace_count += 1
            print(f"  [{session['user_id']}] Turn {turn_num}: {turn['user'][:50]}...")

    print(f"\n  Logged {trace_count} traces across {len(sessions)} sessions (no LLM calls)")
    return trace_count
