# Security Policy

`prodjlink-rs` is a local network protocol crate. It should not make internet calls or contact Compeller services.

## Reporting a vulnerability

Please report security issues privately to Compeller before opening a public issue.

Email: security@compeller.ai

Include:

- affected version or commit
- reproduction steps
- expected impact
- whether the issue requires access to a Pro DJ Link LAN

## Scope

In scope:

- unexpected network behavior
- packet parsing crashes
- denial-of-service issues from malformed LAN packets
- accidental data leakage from local track/device metadata

Out of scope:

- issues requiring physical control of your DJ hardware
- normal Pro DJ Link broadcast behavior documented by the crate
