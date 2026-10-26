-- M9 live stream wakeup. Inserts notify one shared listener after commit.
-- The notification is a wakeup. Readers query the stream log themselves.

CREATE OR REPLACE FUNCTION relay_notify_stream()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
BEGIN
  PERFORM pg_notify('relay_stream', NEW.run_id::text);
  RETURN NULL;
END;
$$;

CREATE TRIGGER run_stream_notify
  AFTER INSERT ON run_stream_events
  FOR EACH ROW
  EXECUTE FUNCTION relay_notify_stream();

REVOKE ALL ON FUNCTION relay_notify_stream() FROM PUBLIC;
