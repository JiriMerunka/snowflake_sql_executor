CREATE OR REPLACE VIEW vw_d_user
AS
SELECT
    user_pk,
    user_hash,
    user_id,
    user_email,
    user_type,
    user_group
FROM d_user
WHERE _sys_is_deleted = false AND _sys_is_current = true