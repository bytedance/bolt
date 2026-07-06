---
title: Overview
nav_order: 1
---

<img class="bolt-hero-logo" src="{{ '/static/logo.png' | relative_url }}" alt="Bolt logo">

<div class="bolt-overview-lead">
  <p>Bolt is a C++ native acceleration library for modern analytical systems. It provides composable and extensible execution components that can be integrated with multiple compute frameworks, deployed on diverse hardware platforms, and connected to different data sources.</p>
  <p>Instead of targeting a single engine, Bolt focuses on the physical execution layer of analytical workloads. Its goal is to provide a common native foundation for building high-performance, production-ready data processing systems.</p>
</div>

<div class="bolt-cards">
  <div class="card">
    <h3>🔌 Any Framework, Any Hardware</h3>
    <p>Pluggable into any framework on any hardware to consume any data source — validated on Spark, Flink, Presto and ElasticSearch.</p>
  </div>
  <div class="card">
    <h3>✅ Enterprise-Grade</h3>
    <p>Enterprise-grade performance, result consistency and feature parity with minimal code changes.</p>
  </div>
  <div class="card">
    <h3>🌐 Open Source-First</h3>
    <p>Open governance and public CI — the repo is the source of truth, in the "Community over Code" spirit.</p>
  </div>
</div>

---

## Why Bolt

Analytical platforms are becoming more heterogeneous. A single organization may run multiple query engines, use different storage formats, and deploy workloads across a mix of hardware environments. This diversity makes it difficult to deliver consistent performance and reliability through engine-specific optimization alone.

Bolt addresses this challenge by moving common performance-critical capabilities into a shared native layer. This allows platform teams to improve execution efficiency across systems while reducing duplicated engineering effort.

## A Unified Native Acceleration Layer

Bolt provides reusable execution building blocks for analytical workloads. These components are designed to be embedded into different engines and adapted to different deployment scenarios.

This model helps separate common execution concerns from engine-specific logic. As a result, systems can share a consistent native execution foundation while still preserving their own query planning, scheduling, and user-facing interfaces.

## Built for the Analytical Ecosystem

Bolt is designed to work with the broader analytical ecosystem, including compute frameworks such as Spark, Flink, Presto, and OpenSearch, as well as common data formats and table systems used in modern data platforms.

This makes Bolt suitable for environments where multiple engines, storage systems, and hardware platforms need to evolve together. It provides a practical foundation for building native acceleration across the full data processing stack.

## Production-Oriented Design

Bolt is built with production requirements in mind. Beyond raw performance, it emphasizes result consistency, operational stability, resource efficiency, and long-term maintainability.

These qualities are critical for large-scale data platforms. Native acceleration must not only make workloads faster, but also remain predictable and dependable in day-to-day production operations.

## Open Collaboration

Bolt is developed with an open and collaborative mindset. It aims to grow through feedback from users, engine developers, and platform teams working on real analytical workloads.

By aligning reusable interfaces with ecosystem integration, Bolt can serve as a shared native execution foundation for the broader analytical systems community.

## Summary

Bolt helps modern data platforms build efficient and reliable native execution across diverse analytical workloads. It provides a common C++ acceleration layer for multiple engines, hardware platforms, and data sources, while keeping production readiness and ecosystem extensibility at the center of its design.
{: .fs-5 .fw-500 }
