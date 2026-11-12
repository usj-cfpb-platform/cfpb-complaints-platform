"""Streamlit dashboard (placeholder for Turn 6). Shows service health only, no data yet."""

from __future__ import annotations

import os

import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError

st.set_page_config(page_title="CFPB Complaints Platform", layout="wide")
st.title("Banking Complaints Data Platform")
st.caption("Placeholder: analytical views are built in Turn 6.")


@st.cache_resource
def mongo_client() -> MongoClient:
    return MongoClient(os.environ.get("MONGO_URI", ""), serverSelectionTimeoutMS=2000)


def mongo_status() -> tuple[bool, str]:
    try:
        mongo_client().admin.command("ping")
        return True, "MongoDB reachable"
    except PyMongoError as exc:
        return False, f"MongoDB unavailable: {type(exc).__name__}"


ok, message = mongo_status()
(st.success if ok else st.error)(message)

st.info(
    "Complaint counts reflect complaints submitted to and published by the CFPB. "
    "They do not measure customers, market share, harm or service quality."
)
