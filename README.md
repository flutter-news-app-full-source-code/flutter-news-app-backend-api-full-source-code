<p align="center">
<img src="https://img.shields.io/badge/coverage-50%25-green?style=for-the-badge" alt="coverage">
<a href="https://github.com/veritai-news/veritai-api/wiki/Local-Setup"><img src="https://img.shields.io/badge/DOCUMENTATION-READ-slategray?style=for-the-badge" alt="Documentation: Read"></a>
<a href="LICENSE"><img src="https://img.shields.io/badge/TRIAL_LICENSE-VIEW_TERMS-blue?style=for-the-badge" alt="Trial License: View Terms"></a>
<a href="https://github.com/sponsors/veritai-news"><img src="https://img.shields.io/badge/LIFETIME_LICENSE-PURCHASE-purple?style=for-the-badge" alt="Lifetime License: Purchase"></a>
</p>

---

Part of the **[veritai News Stack](https://github.com/veritai-news)**.

The veritai API is a high-performance backend gateway built with **Dart Frog**. It serves as the secure data engine for both the Mobile Client and the Web Dashboard.

---

## ⭐ Feature Showcase: Everything You Get, Ready to Go

Explore the high-level domains below to see how.

<details>
<summary><strong>🔐 Identity & Access Management</strong></summary>

### 🛡️ Modern, Secure Authentication
A complete identity system provides a frictionless and secure user journey from the very first interaction.
- **Flexible Onboarding:** Supports modern, passwordless sign-in for registered users and seamless anonymous access for guests, reducing barriers to entry.
- **Intelligent Account Conversion:** Automatically migrates all user data—including preferences and saved content—when a guest user creates a permanent account.
- **Robust Session Control:** Uses industry-standard JWTs for stateless sessions and includes a token blacklisting service to ensure sessions are instantly and securely terminated upon sign-out.
> **Your Advantage:** You get a complete, modern, and secure user management system out of the box, covering the entire user lifecycle from guest to registered user.

---

### 👮 Granular, Role-Based Security
A sophisticated and flexible security model ensures that users and administrators can only access and modify the data they are permitted to.
- **Multi-Layered Access Control:** Defines distinct permission sets for different user classes, such as mobile app consumers and dashboard administrators, ensuring a clear separation of capabilities.
- **Automated Ownership Enforcement:** Built-in middleware automatically verifies data ownership before any modification or deletion request is processed, preventing unauthorized actions.
> **Your Advantage:** Easily enforce complex business rules and security policies. The architecture guarantees data integrity and provides a secure foundation for scaling your user base.

---

### 🚦 Automated API Protection
The API is shielded from common threats with intelligent, built-in abuse prevention mechanisms.
- **Smart Rate Limiting:** Protects critical endpoints from brute-force attacks and denial-of-service attempts by applying fair and effective limits based on IP address for guests and user ID for authenticated users.
> **Your Advantage:** Ensure high availability and stability for your application. This automated defense layer protects your infrastructure and preserves a quality experience for legitimate users.

</details>

<details>
<summary><strong>📦 Dynamic Content & Data API</strong></summary>

### ⚙️ A Radically Efficient Data Engine
Instead of a rigid collection of hardcoded routes, the API is built around a single, unified data gateway. This metadata-driven architecture dramatically accelerates development and enhances scalability.
- **Unified Data Endpoint:** A central engine handles all data operations (CRUD) for every data type in the system—from articles and topics to user preferences and beyond.
- **Metadata-Driven Logic:** To add a completely new data type to your application, you simply define its rules—permissions, validation, and database connections—in a central registry. The engine handles the rest automatically.
> **Your Advantage:** This architecture eliminates boilerplate code and massively speeds up development. You can add new features and data models to your application without writing new API routes, enabling you to innovate and scale at a much faster pace.

---

### 🔍 Advanced Querying & Performance
The data API is equipped with powerful querying capabilities, enabling rich, high-performance content discovery features in your client applications.
- **Complex Filtering & Sorting:** Supports deep, multi-parameter filtering and flexible, multi-field sorting directly through the API.
- **High-Performance Pagination:** Utilizes efficient cursor-based pagination to handle massive datasets gracefully, perfect for infinite-scrolling feeds.
> **Your Advantage:** Empower your mobile and web clients with powerful data discovery features right out of the box, without needing to write any extra backend logic.

---

### 🌍 Natively Multilingual & Context-Aware
The data engine is built for global scale from the ground up.
- **Deep Content Localization:** All content models support full translation maps, allowing you to manage multiple languages from a single source of truth.
- **Smart Response Projection:** The API intelligently adapts to the client. It sends optimized, single-language payloads to the mobile app for performance, while delivering raw, multi-language data to the CMS for easy editing.
> **Your Advantage:** Scale your news platform globally without architectural friction. Serve a diverse user base with a system that treats localization as a core feature, not an afterthought.

---

### 🛡️ Robust & Automated Validation
The API automatically validates the structure of all incoming data, ensuring that every request is well-formed before it's processed. This built-in mechanism catches missing fields, incorrect data types, and invalid enum values at the gateway, providing clear, immediate feedback to the client.
> **Your Advantage:** This eliminates an entire class of runtime errors and saves you from writing tedious, repetitive validation code. Your data models remain consistent and your API stays resilient against malformed requests.

</details>

<details>
<summary><strong>🧠 Native Intelligence Engine</strong></summary>

### ⚡ Autonomous Intelligence & Data Enrichment
A standalone, model-agnostic intelligence engine that transforms raw data into structured knowledge. While deeply integrated with the content pipeline, it operates as an independent service layer capable of powering diverse AI workflows beyond simple news processing.
- **Provider-Agnostic Core:** Built on the Strategy Pattern, the engine completely decouples business logic from the underlying model. Switch between OpenAI, Google Gemini, Anthropic, or Mistral instantly without code changes to optimize for cost or performance.
- **Asynchronous Enrichment Worker:** A dedicated background worker acts as a "smart consumer," continuously polling for raw `ingested` content to apply heavy processing—junk filtering, entity extraction, translation, and topic inference—without blocking the main ingestion pipeline.
- **Precision & Governance:** Uses highly specialized, strictly-typed AI strategies per entity (e.g., `HeadlineEnrichment`, `SourceEnrichment`) to enforce rigid JSON schemas and prevent hallucination. A centralized governance layer enforces strict daily token quotas to prevent billing surprises.
- **Automated Editorial Watchdog:** The engine autonomously evaluates content for urgency, identifying high-confidence breaking news and triggering intelligent, deduplicated push notifications to relevant subscribers 24/7.
> **Your Advantage:** Deploy AI features with strategic and financial confidence. Avoid vendor lock-in, leverage the most powerful and cost-effective models on the market, and maintain complete control over your operational spend.

</details>

<details>
<summary><strong>🤖 Automated News Ingestion & Orchestration</strong></summary>

### 🚀 High-Velocity Content Acquisition
A robust, fault-tolerant orchestration pipeline focused purely on the rapid acquisition and staging of external data. It acts as the "mouth" of the system, capturing raw content at scale and normalizing it for downstream AI auditing.
- **Multi-Provider Polymorphism:** Out-of-the-box support for **NewsAPI** and **MediaStack**, utilizing a unified interface that enables hot-swappable ingestion strategies via environment configuration.
- **AI-Delegated Quality Control:** Replaces brittle, static validation logic with context-aware AI auditing. This ensures the acquisition layer remains lightweight and "uncloggable," delegating junk filtering and content enrichement to the Intelligence Engine.
- **On-Demand Source Discovery:** Automatically synchronizes internal sources with provider catalogs using an intelligent host-matching heuristic, eliminating manual mapping overhead.
- **Decoupled Staging Architecture:** Implements a high-performance "Worker-Queue" pattern. Content is captured rapidly and saved as `ingested` status, creating a clean separation of concerns: Ingestion handles raw volume, while the Intelligence Engine extracts high-fidelity value.
- **Intelligent Cost Control:** Optimizes API credit usage through high-density batch processing and strictly enforced daily quotas.
- **Resilient Pipeline Architecture:** Implements a "Poison Pill" isolation strategy that automatically de-batches and disables problematic sources to prevent upstream failures from halting the entire ingestion cycle.
- **Enterprise Deduplication:** Employs a high-performance idempotency layer to guarantee a unique, high-quality content feed for your users.
> **Your Advantage:** A data pipeline that is impossible to clog. By decoupling acquisition from intelligence, the system ensures that slow AI processing or network latency never impacts the freshness or availability of your raw content feed.

---

</details>

<details>
<summary><strong>📧 Email & Transactional Messaging</strong></summary>

### 📨 Reliable Email Delivery
A flexible email infrastructure ensures critical transactional messages, like OTPs, reach your users.
- **Multi-Provider Strategy:** Built-in support for **SendGrid** and **OneSignal**. Simply configure your preferred provider via environment variables.
- **Template-Based Workflow:** Decouples content from code by using provider-side templates (SendGrid Dynamic Templates or OneSignal Templates), allowing you to update email designs without redeploying the server.
- **Unified Interface:** A clean `EmailService` abstraction allows you to swap or add new providers easily without affecting the rest of the application logic.
> **Your Advantage:** Ensure reliable delivery of sign-in codes and alerts with a system that adapts to your preferred infrastructure provider.

</details>

<details>
<summary><strong>📲 User Engagement & Notifications</strong></summary>

### 🔔 A Dynamic, Multi-Channel Notification Engine
A complete, multi-provider notification engine empowers you to engage users with timely and relevant alerts, seamlessly integrated into their app experience.
- **Editorial-Driven Alerts:** Designate any content as "breaking news" from the dashboard to trigger immediate, high-priority push notifications to all relevant subscribers.
- **User-Crafted Notification Streams:** Allow users to create and save persistent filters based on any combination of content attributes (such as topics or sources) and subscribe to receive notifications that match their exact interests.
- **Centralized In-App Inbox:** Every push notification is automatically captured as a persistent in-app message, giving users a central place to catch up on alerts they may have missed.
- **Provider Agnostic & Scalable:** The engine is built to be provider-agnostic, with out-of-the-box support for Firebase (FCM) and OneSignal. The active provider can be switched remotely without any code changes.
- **Fully Localized Delivery:** Notifications are automatically composed and delivered in each user's preferred language, ensuring a native and engaging experience for a global audience.
- **Intelligent, Self-Healing Delivery:** The system is designed for long-term efficiency. It automatically detects and prunes invalid device tokens—for example, when a user uninstalls the app—ensuring your delivery infrastructure remains clean and performant.
> **Your Advantage:** Drive user re-engagement with a powerful and flexible notification system that delivers both broad-reaching alerts and deeply personalized content streams, all built on a scalable, self-healing, and provider-agnostic architecture.

</details>

<details>
<summary><strong>🎁 Rewards & Incentives</strong></summary>

### 🏆 Time-Based Reward System
A secure, server-side verified reward system that incentivizes user engagement (e.g., watching ads) with tangible benefits.
- **Multi-Provider Server-Side Verification (SSV):** Securely verifies reward callbacks from **Google AdMob** (ECDSA) and more, ensuring that every reward grant originates directly from the ad network's servers.
- **Remote Config Driven:** The value and duration of rewards (e.g., "24 Hours Ad-Free") are controlled entirely by your Remote Config, acting as the single source of truth. This decouples business logic from ad network settings.
- **Idempotent Processing:** A dedicated idempotency layer ensures that each reward transaction is processed exactly once, preventing duplicate grants even if the ad network retries callbacks.
> **Your Advantage:** Safely monetize your app with rewarded ads from multiple providers, knowing that your premium features are protected by banking-grade verification logic.

</details>

<details>
<summary><strong>🗄️ Media & Storage Management</strong></summary>

### 🛡️ A Robust, Multi-Provider Media System
A complete media system designed for security, scalability, and cost control. It implements the **Strategy Pattern** to seamlessly switch between enterprise-grade cloud providers and a production-ready local storage solution.

#### Enterprise-Grade Cloud Integration (GCS & S3)
- **Provider Agnostic:** Avoid vendor lock-in with out-of-the-box support for **Google Cloud Storage (GCS)** and **AWS S3**. Switch between them with a single environment variable.
- **Secure Direct-to-Cloud Uploads:** The system offloads all binary traffic from your API server by using **pre-signed URLs** (V4 Policy). Clients upload files directly to the cloud, keeping your API lightweight, secure, and scalable.
- **Event-Driven Finalization:** Your database is updated only after receiving a cryptographically secure webhook confirmation from the cloud provider, guaranteeing data consistency and preventing "ghost" assets or broken links.

#### Production-Ready Local Storage Provider
- **Seamless Dev/Prod Parity:** The local provider perfectly emulates the asynchronous, two-stage upload flow of its cloud counterparts. It uses single-use tokens for authorization and a dedicated background worker for finalization, ensuring your application logic remains identical across all environments.
- **Asynchronous Finalization:** The local provider emulates the asynchronous, webhook-based finalization of cloud providers. After an upload, it queues a job for a separate background worker. This decouples the upload request from the subsequent database updates, ensuring the API endpoint returns immediately and the finalization logic happens reliably in the background.
- **Zero-Configuration for Docker:** When running with the provided Docker setup, the local storage provider works out of the box with a pre-configured persistent volume.

#### Automated Lifecycle & Cost Control
- **Automated Hygiene:** A dedicated, schedulable worker process automatically detects and prunes orphaned files and stale pending uploads. This keeps your storage buckets clean and your costs optimized by ensuring you never pay for unused assets.
> **Your Advantage:** You get an enterprise-grade asset management pipeline that is secure, scalable, and cost-efficient. Whether you're developing locally or deploying to the cloud, the architecture provides a consistent, reliable, and professional solution for handling all media and user-generated content.

</details>

<details>
<summary><strong>📊 Insightful Analytics System</strong></summary>

### 📈 A Unified Business Intelligence System
A complete, multi-provider analytics system that transforms raw data from both external services and your own application database into insightful, aggregated metrics for your dashboard.
- **Dual-Source ETL:** A standalone worker process runs on a schedule to perform a full Extract, Transform, and Load (ETL) operation. It pulls behavioral data from your chosen analytics provider (Google Analytics or Mixpanel) and combines it with operational data by running direct, complex aggregations against the application's own database.
- **High-Performance Dashboard:** The web dashboard reads this pre-aggregated data, resulting in near-instant load times for all analytics charts and metrics. This architecture avoids slow, direct, on-the-fly queries from the client to the analytics provider.
- **Provider-Agnostic & Extensible:** The system is built on a clean, abstract interface, decoupling the core logic from any specific provider. Switch between Google Analytics and Mixpanel with a simple configuration change, or integrate a new provider by implementing a single, well-defined contract. Adding new charts or KPIs is as simple as defining a new metric mapping.
> **Your Advantage:** Get a complete, production-grade BI pipeline out of the box. Deliver a fast, responsive dashboard and gain a holistic view of your business by combining user behavior analytics with real-time operational metrics—a capability that external analytics tools alone cannot provide.

</details>

<details>
<summary><strong>🏗️ Architecture & Infrastructure</strong></summary>

### 🚀 High-Performance by Design
- **Dart Frog Core:** Leverages the high-performance Dart Frog framework for a fast, efficient, and scalable backend.
- **Clean, Layered Architecture:** A strict separation of concerns into distinct layers makes the codebase clean, maintainable, and easy to reason about.
> **Your Advantage:** Your backend is built on a solid, modern foundation that is both powerful and a pleasure to work with, reducing maintenance overhead.

---

### 🔌 Extensible & Unlocked
The entire application is designed with a robust dependency injection system, giving you the freedom to choose your own infrastructure.
- **Swappable Implementations:** Easily swap out core components—like the database, email provider, or file storage service—without rewriting business logic.
> **Your Advantage:** Avoid vendor lock-in and future-proof your application. You have the freedom to adapt and evolve your tech stack as your business needs change.

---

### 🔄 Automated & Traceable Database Migrations
Say goodbye to risky manual database updates. A professional, versioned migration system ensures your database schema evolves safely and automatically.
- **Code-Driven Schema Evolution:** The system automatically applies schema changes to your database on application startup, ensuring consistency across all environments.
- **Traceable to Source:** Each migration is versioned and directly linked to the pull request that initiated it, providing a clear, auditable history of every change.
> **Your Advantage:** Deploy with confidence. This robust system eliminates an entire class of deployment errors, ensuring your data models evolve gracefully and reliably with full traceability.

</details>

## 🚀 Getting Started & Running Locally

For a complete guide on setting up your local environment and running this project, please see the [Wiki](https://github.com/veritai-news/veritai-api/wiki) tab in this repository.

## 🔑 Licensing

This source code is licensed for commercial use and is provided for local evaluation. A **Lifetime Commercial License** is required for any production or public-facing application.

Please visit the main [veritai News Stack](https://github.com/veritai-news) organization page to review the full license terms and to purchase.
