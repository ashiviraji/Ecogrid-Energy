""EcoGrid Energy Trading Platform""

EcoGrid Energy is a Python-based prototype that simulates a peer-to-peer (P2P) renewable energy trading system. The platform allows users with solar panels to sell excess electricity directly to buyers through an event-driven microservices architecture.

This project demonstrates important software architecture concepts including:

Microservices architecture
Event-driven communication
Kafka-style messaging
Saga pattern
Bounded contexts
IoT smart meter integration
Fault tolerance and compensation handling

The system contains multiple independent services such as User Management, Smart Meter Service, Marketplace Service, Financial Settlement Service, and Notification Service. Each service has its own responsibility and communicates through simulated Kafka events.

The Smart Meter Service processes real-time energy readings from IoT devices. The Marketplace Service manages energy offers, buyer bids, and trade matching. The Financial Settlement Service processes payments and handles transaction failures using the Saga pattern. The Notification Service sends alerts and updates to users.

The project is fully implemented in Python using built-in libraries only, without external dependencies or databases. In-memory Python data structures are used to simulate persistence and Kafka communication.

Project Files
main.py
models.py
data_store.py
user_service.py
meter_service.py
marketplace_service.py
settlement_service.py

How to Run

Open the project folder in VS Code and run:

python main.py

Or

py main.py

The system will simulate:

User registration
Smart meter data ingestion
Energy trading
Trade matching
Payment settlement
User notifications

This project was developed for the EcoGrid Energy System Design Assessment to demonstrate scalable and maintainable software architecture principles using Python.