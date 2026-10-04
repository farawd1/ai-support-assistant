CREATE TABLE IF NOT EXISTS accounts(id BIGSERIAL PRIMARY KEY,username TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,role TEXT NOT NULL CHECK(role IN ('user','operator')));
CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY,account_id BIGINT NOT NULL REFERENCES accounts(id),expires DOUBLE PRECISION NOT NULL);
CREATE INDEX IF NOT EXISTS sessions_expiration ON sessions(expires);
CREATE TABLE IF NOT EXISTS audit(id BIGSERIAL PRIMARY KEY,created DOUBLE PRECISION NOT NULL,actor TEXT NOT NULL,event TEXT NOT NULL,target TEXT NOT NULL,detail TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS audit_time ON audit(created,id);
CREATE TABLE IF NOT EXISTS tickets(
 id TEXT PRIMARY KEY,text TEXT NOT NULL,created DOUBLE PRECISION NOT NULL,state TEXT NOT NULL,result TEXT,error TEXT,action TEXT,final_reply TEXT,latency DOUBLE PRECISION,
 source TEXT NOT NULL DEFAULT 'manual',received_at DOUBLE PRECISION,analyzed_at DOUBLE PRECISION,topic_id BIGINT,reviewed_at DOUBLE PRECISION,decision_actor TEXT,
 processing_started DOUBLE PRECISION,processing_token TEXT,auto_started DOUBLE PRECISION,
 auto_state TEXT NOT NULL DEFAULT 'pending',auto_decision TEXT,owner_id BIGINT REFERENCES accounts(id)
);
CREATE TABLE IF NOT EXISTS actions(id BIGSERIAL PRIMARY KEY,ticket_id TEXT NOT NULL,action TEXT NOT NULL,reply TEXT NOT NULL,created DOUBLE PRECISION NOT NULL,actor TEXT NOT NULL DEFAULT 'human',reason TEXT NOT NULL DEFAULT '');
CREATE INDEX IF NOT EXISTS actions_ticket ON actions(ticket_id,id);
CREATE TABLE IF NOT EXISTS topics(id BIGSERIAL PRIMARY KEY,title TEXT NOT NULL,normalized TEXT UNIQUE NOT NULL,description TEXT NOT NULL,created DOUBLE PRECISION NOT NULL,model TEXT NOT NULL,evidence_ids TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT INTO settings VALUES('autopilot','off'),('autopilot_revision','0'),('live-v2','postgres') ON CONFLICT DO NOTHING;
CREATE OR REPLACE FUNCTION repl_audit_decision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO audit(created,actor,event,target,detail) VALUES(NEW.created,NEW.actor,'decision',NEW.ticket_id,NEW.action || ': ' || NEW.reason);
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS audit_decisions ON actions;
CREATE TRIGGER audit_decisions AFTER INSERT ON actions FOR EACH ROW EXECUTE FUNCTION repl_audit_decision();
CREATE OR REPLACE FUNCTION repl_audit_analysis() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state IN ('ready','failed') AND OLD.state <> NEW.state THEN
  INSERT INTO audit(created,actor,event,target,detail) VALUES(EXTRACT(EPOCH FROM clock_timestamp()),'system','analysis',NEW.id,NEW.state);
 END IF;
 RETURN NEW;
END; $$;
DROP TRIGGER IF EXISTS audit_analysis ON tickets;
CREATE TRIGGER audit_analysis AFTER UPDATE OF state ON tickets FOR EACH ROW EXECUTE FUNCTION repl_audit_analysis();
