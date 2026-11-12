-- What RoleLogic's dashboard has been told about each role link, so an owner
-- whose channel stopped working sees it on the role link itself instead of
-- only inside the settings page they have no reason to open.
--
-- `reported_notice` is the notice RoleLogic last accepted: 'none' or a notice
-- code. The notice worker compares it with what the link needs right now and
-- sends the difference. Every existing row starts at 'none' because RoleLogic
-- has never held a notice for any of them; the links that need one are then
-- reported on the worker's first pass. `notice_retry_at` paces a link
-- RoleLogic could not take a notice for yet (an older API, a link that is
-- switched off).
ALTER TABLE role_links ADD COLUMN IF NOT EXISTS reported_notice TEXT NOT NULL DEFAULT 'none';
ALTER TABLE role_links ADD COLUMN IF NOT EXISTS notice_retry_at TIMESTAMPTZ;

-- When a manager last pressed "Re-check everyone now" for this link. Bounds
-- how often one link can queue a full re-check of its server.
ALTER TABLE role_links ADD COLUMN IF NOT EXISTS recheck_requested_at TIMESTAMPTZ;
