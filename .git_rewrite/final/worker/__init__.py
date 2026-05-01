"""
DistroSync Worker Package

Workers are the execution engines that pull tasks from the broker
and process them. They include heartbeat monitoring so the broker
knows they're alive, and ACK/NACK responses so the broker knows
whether tasks succeeded or failed.
"""
