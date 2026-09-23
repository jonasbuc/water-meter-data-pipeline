-- =========================================================
-- MIGRATION 003: pipeline failure diagnostics
--
-- pipeline_runs kunne allerede fortælle AT en kørsel fejlede (status =
-- 'FAILED'), men ikke HVOR i pipelinen eller HVORFOR på et overordnet
-- niveau. SQLite understøtter "ALTER TABLE ... ADD COLUMN" uden
-- tabel-ombygning, så dette er en billig, additiv migration.
--
-- Vi gemmer KUN et kort, operationelt sammendrag her - ikke et fuldt
-- stack trace. Det fulde stack trace hører til i Python-loggeren
-- (logger.exception i pipeline.py), ikke i databasen.
-- =========================================================

ALTER TABLE pipeline_runs ADD COLUMN failed_stage TEXT;
ALTER TABLE pipeline_runs ADD COLUMN error_type TEXT;
ALTER TABLE pipeline_runs ADD COLUMN error_message TEXT;
