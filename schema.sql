-- Habilita extensão para Geo-localização (Vital para a tese)
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS telemetria_ev (
    id SERIAL PRIMARY KEY,
    -- Nomes alinhados com o script Python (evita erro de coluna não encontrada)
    vehicle_id VARCHAR(50),      
    battery_level FLOAT,
    temperature FLOAT,
    speed_kmh FLOAT,
    
    -- Coluna espacial correta
    location GEOGRAPHY(POINT, 4326),
    
    -- Timestamps
    ts_envio TIMESTAMPTZ NOT NULL,       -- Latência calculada via to_timestamp no Python
    ts_processamento TIMESTAMPTZ DEFAULT NOW()
);

-- Índices para performance
CREATE INDEX idx_ts_envio ON telemetria_ev(ts_envio DESC);