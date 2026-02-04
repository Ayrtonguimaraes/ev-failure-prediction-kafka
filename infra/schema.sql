CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS telemetria_ev (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50),
    battery_level FLOAT,
    temperature FLOAT,
    speed_kmh FLOAT,
    location GEOMETRY(POINT, 4326),
    ts_envio TIMESTAMP,
    ts_persistencia TIMESTAMP DEFAULT NOW() -- Para auditar latência interna do banco
);

-- Índice para consultas espaciais rápidas (Speed Layer geográfica)
CREATE INDEX IF NOT EXISTS idx_telemetria_location ON telemetria_ev USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_telemetria_ts ON telemetria_ev (ts_envio);