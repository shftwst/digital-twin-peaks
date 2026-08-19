CREATE USER control_user PASSWORD 'control_local_only';
CREATE USER relay_user PASSWORD 'relay_local_only';
CREATE USER identity_user PASSWORD 'identity_local_only';
CREATE USER crm_user PASSWORD 'crm_local_only';
CREATE USER platform_test_user PASSWORD 'platform_test_local_only';

CREATE DATABASE control OWNER control_user;
CREATE DATABASE relay OWNER relay_user;
CREATE DATABASE identity OWNER identity_user;
CREATE DATABASE crm OWNER crm_user;
CREATE DATABASE platform_test OWNER platform_test_user;

REVOKE CONNECT ON DATABASE control FROM PUBLIC;
REVOKE CONNECT ON DATABASE relay FROM PUBLIC;
REVOKE CONNECT ON DATABASE identity FROM PUBLIC;
REVOKE CONNECT ON DATABASE crm FROM PUBLIC;
REVOKE CONNECT ON DATABASE platform_test FROM PUBLIC;
GRANT CONNECT ON DATABASE control TO control_user;
GRANT CONNECT ON DATABASE relay TO relay_user;
GRANT CONNECT ON DATABASE identity TO identity_user;
GRANT CONNECT ON DATABASE crm TO crm_user;
GRANT CONNECT ON DATABASE platform_test TO platform_test_user;
