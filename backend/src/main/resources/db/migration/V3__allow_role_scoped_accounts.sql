ALTER TABLE app_user DROP CONSTRAINT IF EXISTS app_user_username_key;
ALTER TABLE app_user ADD CONSTRAINT app_user_username_role_key UNIQUE (username, role);
