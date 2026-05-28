-- Schema de transacciones procesadas desde SFTP
CREATE TABLE IF NOT EXISTS transacciones (
    id              SERIAL PRIMARY KEY,
    transaccion_id  VARCHAR(20)    NOT NULL UNIQUE,
    rut             VARCHAR(12)    NOT NULL,
    nombre          VARCHAR(100)   NOT NULL,
    tipo            VARCHAR(30)    NOT NULL,
    monto           NUMERIC(14, 2) NOT NULL,
    moneda          VARCHAR(5)     DEFAULT 'CLP',
    comercio        VARCHAR(100),
    region          VARCHAR(50)    NOT NULL,
    estado          VARCHAR(20)    NOT NULL,
    fecha           DATE           NOT NULL,
    procesado_en    TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transacciones_rut   ON transacciones(rut);
CREATE INDEX IF NOT EXISTS idx_transacciones_fecha ON transacciones(fecha);
CREATE INDEX IF NOT EXISTS idx_transacciones_tipo  ON transacciones(tipo);
