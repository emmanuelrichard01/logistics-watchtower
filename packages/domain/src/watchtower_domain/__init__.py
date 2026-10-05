"""Watchtower domain logic.

Everything here is a pure function of its inputs, shaped as
``evaluate(state, event, rules) -> (new_state, outputs)``. The stream processor,
the edge agent and the replay tool all call the same code, so it must never
import Kafka, Postgres, HTTP or any other I/O library (rebuild plan, section 17).
"""
