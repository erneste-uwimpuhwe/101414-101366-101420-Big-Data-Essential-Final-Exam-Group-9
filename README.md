# RwandaRide

**A real-time ride-monitoring data pipeline with machine-learning ETA prediction.**

Big Data Essentials — Group Final Exam Project  
Adventist University of Central Africa, Faculty of Information Technology  
Lecturer: **Dr Kundan Kumar**

| Student ID | Full Name |
|---|---|
| 101420 | Patrick **NIYONSHUTI** |
| 101414 | Erneste **UWIMPUHWE** |
| 101366 | Gisele **UWIBEREYEHO** |

---

## The problem

Ride-monitoring dispatch is a decision problem under time pressure. A dispatcher watching a live fleet needs two things at once:

1. **Which trips are running materially behind**, so a rider can be called — this needs *every event as it arrives*.
2. **Where and when demand is about to concentrate**, so drivers can be repositioned *before* the surge — this needs the *entire history aggregated*.

Neither question can be answered from a single database row, and no single store answers both well. That is why this project has the architecture it has.

> The Kafka stream carries trip events as they happen, the Spark MLlib model predicts trip duration in minutes, and the dashboard shows dispatchers which trips are running late and which zones are short on drivers.

The domain is modelled on Kigali: eight zones, sixty drivers, fares in Rwandan francs.

---

## Architecture
