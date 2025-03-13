# **📜 Minimal Streaming SQL Service**  

🚀 **A Streaming SQL Service built with Rust and Differential Dataflow**  

---

## 📌 **Table of Contents**
- [About the Project](#about-the-project)
- [Key Features](#key-features)
- [Tech Stack](#tech-stack)
- [License](#license)

---

## 📖 **About the Project**  

This project is part of my **Master’s Thesis**, where I designed and implemented a **minimal Streaming SQL Service** using **Rust** and **Differential Dataflow**. The system processes continuous data streams, executes SQL-like queries in real-time, and leverages **incremental computation** for efficient updates.  

🔹 **Why Streaming SQL?**  
Traditional SQL databases struggle with **real-time stream processing**, requiring specialized solutions like **Flink** or **Materialize**. This project explores an **alternative minimal approach** using Rust’s powerful dataflow libraries.  

🔹 **Goals of the Project:**  
✅ Implement a lightweight **SQL-like interface** for continuous queries  
✅ Utilize **Differential Dataflow** for efficient incremental computation  
✅ Provide a **low-latency**, **high-throughput** streaming processing engine  

---

## 🌟 **Key Features**  

✅ **Streaming SQL Query Execution** – Run continuous queries on real-time data streams (limited to JOINs and WHERE)
✅ **Incremental Computation** – Uses **Differential Dataflow** for efficient updates  
✅ **Minimal & Lightweight** – Focuses on core functionality without unnecessary complexity  
✅ **Rust-Powered Performance** – Memory-safe, efficient, and highly concurrent  

---

## 🛠 **Tech Stack**  

🚀 **Core Technologies:**  
- **Rust** – Memory safety, concurrency, and performance  
- **Differential Dataflow** – Incremental computation framework  
- **Timely Dataflow** – Stream processing model for parallel computation  

🔗 **Additional Components:**  
- **Serde & JSON** – For efficient serialization & deserialization  
- **Tokio** – Asynchronous runtime for handling concurrent streams  
- **PostgreSQL** – For query persistence  

---

## 📜 **License**  
This project is licensed under the **MIT License** – see the [LICENSE](LICENSE) file for details.  

---

⭐ **If you found this project helpful, consider giving it a star!** 🚀  
