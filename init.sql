DROP TABLE IF EXISTS fraud_alerts;

CREATE TABLE fraud_alerts (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(20),
    amount DECIMAL(10,2),
    country VARCHAR(5),
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    fraud_reason VARCHAR(100),
    detected_at TIMESTAMP DEFAULT NOW()
);
