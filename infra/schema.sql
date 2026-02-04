CREATE TABLE IF NOT EXISTS telemetria (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50),
    velocidade INT,
    bateria INT,
    temperatura_motor FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    ts_sensor TIMESTAMP, -- Hora que o dado nasceu no carro
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Hora que salvamos no banco
);

-- Cria um índice para deixar as consultas por carro mais rápidas
CREATE INDEX idx_vehicle_id ON telemetria(vehicle_id);