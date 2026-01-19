-- Instrução para criar a tabela de telemetria
CREATE TABLE IF NOT EXISTS telemetria_ev (
    id SERIAL PRIMARY KEY,
    car_id VARCHAR(50),
    bateria FLOAT,
    temperatura FLOAT,
    velocidade FLOAT,
    ts_envio DOUBLE PRECISION,      -- Timestamp de quando o sensor enviou
    ts_recebimento DOUBLE PRECISION -- Timestamp de quando o sistema processou
);