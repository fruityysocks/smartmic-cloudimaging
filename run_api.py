#!/usr/bin/env python3
"""
Entry point for running the AWS HealthImaging API server.

Usage:
    python run_api.py
"""

from src.api import app

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9090, debug=True)
