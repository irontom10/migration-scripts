\
#!/usr/bin/env php
<?php
/**
 * migrate_entities_v3.php
 *
 * Migrates legacy party-like tables from shop_db_1 into an "entities + roles" model in shop_db_6.
 *
 * Source tables (shop_db_1):
 *   - tblCustomers
 *   - tblVendors
 *   - tblEmployees
 *   - DISTINCT tblWOSublet.SubletCompany
 *
 * Target tables (shop_db_6):
 *   - entities, entity_kinds
 *   - entity_persons, entity_organizations, org_types
 *   - entity_roles, entity_role_map
 *   - entity_addresses (+ address_types FK)
 *   - entity_phones (+ phone_types FK)
 *   - entity_emails (+ email_types FK)
 *   - customer_accounts + customer_billing + customer_tax_info (+ customer_types, pricing_plans)
 *   - vendor_accounts
 *   - employee_accounts (+ employee_statuses)
 *   - sublet_accounts
 *
 * Usage:
 *   php migrate_entities_v3.php create
 *   php migrate_entities_v3.php drop
 *   php migrate_entities_v3.php migrate [--dry-run]
 *
 * Env overrides:
 *   DB_HOST, DB_USER, DB_PASS, DB_PORT, SRC_DB, DST_DB
 */

error_reporting(E_ALL);
ini_set('display_errors', '1');

$DB_HOST = getenv('DB_HOST') ?: '127.0.0.1';
$DB_USER = getenv('DB_USER') ?: 'errs_user';
$DB_PASS = getenv('DB_PASS') ?: '';
$DB_PORT = intval(getenv('DB_PORT') ?: '3306');
$SRC_DB  = getenv('SRC_DB')  ?: 'shop_db_1';
$DST_DB  = getenv('DST_DB')  ?: 'shop_db_6';

function logmsg(string $msg): void
{
  $ts = date('Y-m-d H:i:s');
  fwrite(STDOUT, "[$ts] $msg\n");
}

function usage(): void
{
  global $argv;
  $me = basename($argv[0]);
  fwrite(STDOUT, "Usage:\n  php $me create\n  php $me drop\n  php $me update [--dry-run]\n  php $me migrate [--dry-run]\n");
  exit(1);
}

function db_connect(string $host, string $user, string $pass, int $port, string $db): mysqli
{
  $m = new mysqli($host, $user, $pass, $db, $port);
  if ($m->connect_error) {
    throw new RuntimeException("DB connect error ($db): " . $m->connect_error);
  }
  $m->set_charset('utf8mb4');
  return $m;
}

function q(mysqli $db, string $sql): mysqli_result|bool
{
  $res = $db->query($sql);
  if ($res === false) {
    throw new RuntimeException("SQL error: " . $db->error . "\nSQL: $sql");
  }
  return $res;
}

function table_exists(mysqli $db, string $schema, string $table): bool
{
  $stmt = $db->prepare("SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1");
  $stmt->bind_param("ss", $schema, $table);
  $stmt->execute();
  $res = $stmt->get_result();
  $exists = $res !== false && $res->fetch_row();
  $stmt->close();
  return (bool)$exists;
}

function index_exists(mysqli $db, string $schema, string $table, string $index): bool
{
  $stmt = $db->prepare("SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1");
  $stmt->bind_param("sss", $schema, $table, $index);
  $stmt->execute();
  $res = $stmt->get_result();
  $exists = $res !== false && $res->fetch_row();
  $stmt->close();
  return (bool)$exists;
}

function fk_exists(mysqli $db, string $schema, string $table, string $fkName): bool
{
  $stmt = $db->prepare("SELECT 1 FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = ? LIMIT 1");
  $stmt->bind_param("sss", $schema, $table, $fkName);
  $stmt->execute();
  $res = $stmt->get_result();
  $exists = $res !== false && $res->fetch_row();
  $stmt->close();
  return (bool)$exists;
}

function ensure_index(mysqli $db, bool $dryRun, string $schema, string $table, string $index, string $definition): void
{
  if (index_exists($db, $schema, $table, $index)) return;
  if ($dryRun) return;
  q($db, "ALTER TABLE $table ADD $definition");
}

function ensure_fk(mysqli $db, bool $dryRun, string $schema, string $table, string $fkName, string $definition): void
{
  if (fk_exists($db, $schema, $table, $fkName)) return;
  if ($dryRun) return;
  q($db, "ALTER TABLE $table ADD CONSTRAINT $fkName $definition");
}

function normalize_key(string $s): string
{
  $s = strtolower(trim($s));
  $s = preg_replace('/\s+/', ' ', $s);
  $s = preg_replace('/[^a-z0-9 \-\.&]/', '', $s);
  return trim($s);
}

function concat_name(string $first, string $last): string
{
  $first = trim($first);
  $last  = trim($last);
  $name = trim($first . ' ' . $last);
  return $name;
}

// -------------------------
// SCHEMA (target)
// -------------------------
$schemaSql = <<<SQL
SET FOREIGN_KEY_CHECKS=0;

-- Lookup for base entity kind (PERSON vs ORG). Used by entities.EntityKindID.
CREATE TABLE IF NOT EXISTS entity_kinds (
  EntityKindID INT NOT NULL PRIMARY KEY,
  Code VARCHAR(20) NOT NULL UNIQUE,
  Description VARCHAR(50) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup for base entity kind (PERSON vs ORG). Used by entities.EntityKindID.';

-- Lookup for organization subtype (BUSINESS, GOVERNMENT, FINANCIAL, etc.). Used by entity_organizations.OrgTypeID.
CREATE TABLE IF NOT EXISTS org_types (
  OrgTypeID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  Code VARCHAR(30) NOT NULL UNIQUE,
  Description VARCHAR(80) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup for organization subtype (BUSINESS, GOVERNMENT, FINANCIAL, etc.). Used by entity_organizations.OrgTypeID.';

-- Master identity table for all parties (people & organizations). Other modules reference EntityID and snapshot DisplayName.
CREATE TABLE IF NOT EXISTS entities (
  EntityID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  EntityKindID INT NOT NULL,
  DisplayName VARCHAR(150) NOT NULL,
  IsActive TINYINT(1) NOT NULL DEFAULT 1,
  CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UpdatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_entity_kind FOREIGN KEY (EntityKindID) REFERENCES entity_kinds(EntityKindID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Master identity table for all parties (people & organizations). Other modules reference EntityID and snapshot DisplayName.';

-- 1:1 extension of entities for PERSON records (names and optional demographics).
CREATE TABLE IF NOT EXISTS entity_persons (
  EntityID INT NOT NULL PRIMARY KEY,
  FirstName VARCHAR(50) NOT NULL,
  MiddleName VARCHAR(50) NULL,
  LastName VARCHAR(50) NOT NULL,
  DOB DATE NULL,
  CONSTRAINT fk_person_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='1:1 extension of entities for PERSON records (names and optional demographics).';

-- 1:1 extension of entities for ORG records (legal/DBA name, tax id, org type).
CREATE TABLE IF NOT EXISTS entity_organizations (
  EntityID INT NOT NULL PRIMARY KEY,
  OrgTypeID INT NOT NULL,
  LegalName VARCHAR(150) NOT NULL,
  DBAName VARCHAR(150) NULL,
  TaxID VARCHAR(20) NULL,
  CONSTRAINT fk_org_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_org_type FOREIGN KEY (OrgTypeID) REFERENCES org_types(OrgTypeID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='1:1 extension of entities for ORG records (legal/DBA name, tax id, org type).';

-- Lookup of roles an entity can have (CUSTOMER, VENDOR, EMPLOYEE, SUBLET_PROVIDER, etc.).
CREATE TABLE IF NOT EXISTS entity_roles (
  RoleID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  Code VARCHAR(30) NOT NULL UNIQUE,
  Description VARCHAR(80) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup of roles an entity can have (CUSTOMER, VENDOR, EMPLOYEE, SUBLET_PROVIDER, etc.).';

-- Many-to-many mapping between entities and roles. Existence of a row grants the role to an entity.
CREATE TABLE IF NOT EXISTS entity_role_map (
  EntityID INT NOT NULL,
  RoleID INT NOT NULL,
  AssignedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (EntityID, RoleID),
  CONSTRAINT fk_rolemap_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_rolemap_role FOREIGN KEY (RoleID) REFERENCES entity_roles(RoleID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Many-to-many mapping between entities and roles. Existence of a row grants the role to an entity.';

-- Lookup of phone labels/types (main, mobile, billing, dispatch, etc.). Used by entity_phones.PhoneTypeID.
CREATE TABLE IF NOT EXISTS phone_types (
  PhoneTypeID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  PhoneTypeName VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup of phone labels/types (main, mobile, billing, dispatch, etc.). Used by entity_phones.PhoneTypeID.';

-- Lookup of email labels/types (main, billing, dispatch, etc.). Used by entity_emails.EmailTypeID.
CREATE TABLE IF NOT EXISTS email_types (
  EmailTypeID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  EmailTypeName VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup of email labels/types (main, billing, dispatch, etc.). Used by entity_emails.EmailTypeID.';

-- Lookup of address labels/types (billing, shipping, service, mailing, etc.). Used by entity_addresses.AddressTypeID.
CREATE TABLE IF NOT EXISTS address_types (
  AddressTypeID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  AddressTypeName VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup of address labels/types (billing, shipping, service, mailing, etc.). Used by entity_addresses.AddressTypeID.';

-- Phone numbers attached to an entity. Multiple per entity; one can be marked primary.
CREATE TABLE IF NOT EXISTS entity_phones (
  PhoneID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  EntityID INT NOT NULL,
  PhoneTypeID INT NOT NULL,
  PhoneNumber VARCHAR(20) NOT NULL,
  IsPrimary TINYINT(1) NOT NULL DEFAULT 0,
  CONSTRAINT fk_phone_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_phone_type FOREIGN KEY (PhoneTypeID) REFERENCES phone_types(PhoneTypeID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Phone numbers attached to an entity. Multiple per entity; one can be marked primary.';

-- Email addresses attached to an entity. Multiple per entity; one can be marked primary.
CREATE TABLE IF NOT EXISTS entity_emails (
  EmailID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  EntityID INT NOT NULL,
  EmailTypeID INT NOT NULL,
  Email VARCHAR(150) NOT NULL,
  IsPrimary TINYINT(1) NOT NULL DEFAULT 0,
  CONSTRAINT fk_email_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_email_type FOREIGN KEY (EmailTypeID) REFERENCES email_types(EmailTypeID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Email addresses attached to an entity. Multiple per entity; one can be marked primary.';

-- Addresses attached to an entity. Supports billing/shipping/service/etc via AddressTypeID.
CREATE TABLE IF NOT EXISTS entity_addresses (
  AddressID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  EntityID INT NOT NULL,
  AddressTypeID INT NOT NULL,
  Address1 VARCHAR(150) NULL,
  Address2 VARCHAR(150) NULL,
  City VARCHAR(50) NULL,
  State VARCHAR(50) NULL,
  Postal VARCHAR(20) NULL,
  Country VARCHAR(50) NULL DEFAULT 'US',
  IsPrimary TINYINT(1) NOT NULL DEFAULT 0,
  UNIQUE KEY uq_addr_primary (EntityID, AddressTypeID, Address1, City, State, Postal),
  CONSTRAINT fk_addr_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_addr_type FOREIGN KEY (AddressTypeID) REFERENCES address_types(AddressTypeID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Addresses attached to an entity. Supports billing/shipping/service/etc via AddressTypeID.';

-- Role profiles
-- Customer role anchor (1:1 with entities when the entity is a customer). Stores legacy IDs and joins customer-specific tables.
CREATE TABLE IF NOT EXISTS customer_accounts (
  EntityID INT NOT NULL PRIMARY KEY,
  LegacyCustomerID INT NULL UNIQUE,
  InternalComments TEXT NULL,
  CONSTRAINT fk_cust_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Customer role anchor (1:1 with entities when the entity is a customer). Stores legacy IDs and joins customer-specific tables.';

-- Lookup for customer classification from legacy TypeID (Charge Account, Retail, Commercial, Wholesale). Used by customer_billing.CustomerTypeID.
CREATE TABLE IF NOT EXISTS customer_types (
  CustomerTypeID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  CustomerTypeName VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup for customer classification from legacy TypeID (Charge Account, Retail, Commercial, Wholesale). Used by customer_billing.CustomerTypeID.';

-- Lookup for default pricing/markup plans (e.g., D with multiplier 1.20). Designed to be extended later.
CREATE TABLE IF NOT EXISTS pricing_plans (
  PricingPlanID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  Code VARCHAR(50) NOT NULL UNIQUE,
  Multiplier DECIMAL(10,4) NOT NULL DEFAULT 1.0000,
  Description VARCHAR(150) NULL,
  IsActive TINYINT(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup for default pricing/markup plans (e.g., D with multiplier 1.20). Designed to be extended later.';

-- Customer billing defaults/settings (customer type, pricing plan, credit & payment acceptance flags).
CREATE TABLE IF NOT EXISTS customer_billing (
  EntityID INT NOT NULL PRIMARY KEY,
  CustomerTypeID INT NULL,
  CreditLimit DECIMAL(12,2) NULL,
  AcceptChecks TINYINT(1) NOT NULL DEFAULT 1,
  AcceptCharge TINYINT(1) NOT NULL DEFAULT 1,
  DefaultPricingPlanID INT NULL,
  Notes TEXT NULL,
  CONSTRAINT fk_cust_bill_entity FOREIGN KEY (EntityID) REFERENCES customer_accounts(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_cust_bill_type FOREIGN KEY (CustomerTypeID) REFERENCES customer_types(CustomerTypeID),
  CONSTRAINT fk_cust_bill_pricing FOREIGN KEY (DefaultPricingPlanID) REFERENCES pricing_plans(PricingPlanID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Customer billing defaults/settings (customer type, pricing plan, credit & payment acceptance flags).';

-- Customer tax profile (tax-exempt flag and resale/certificate number). Split out for future tax expansion.
CREATE TABLE IF NOT EXISTS customer_tax_info (
  EntityID INT NOT NULL PRIMARY KEY,
  TaxExempt TINYINT(1) NOT NULL DEFAULT 0,
  ResaleNo VARCHAR(50) NULL,
  CONSTRAINT fk_cust_tax_entity FOREIGN KEY (EntityID) REFERENCES customer_accounts(EntityID) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Customer tax profile (tax-exempt flag and resale/certificate number). Split out for future tax expansion.';

-- Vendor role profile (1:1 with entities when the entity is a vendor). Holds vendor-specific defaults and legacy IDs.

-- Canonical vendor category / lookup codes (normalized from legacy tblVendors.LookupCode).
-- Vendors reference this via FK so reporting and categorization are consistent.
CREATE TABLE IF NOT EXISTS vendor_lookup_codes (
  VendorLookupCodeID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  Code VARCHAR(50) NOT NULL UNIQUE,
  Description VARCHAR(150) NOT NULL,
  IsActive TINYINT(1) NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Canonical vendor lookup codes/categories. Populated from a mapping of legacy tblVendors.LookupCode values to stable codes used by ERRS.';

CREATE TABLE IF NOT EXISTS vendor_accounts (
  EntityID INT NOT NULL PRIMARY KEY,
  LegacyVendorID INT NULL UNIQUE,
  PaytermsID INT NULL,
  AcctID INT NULL,
  VendorLookupCodeID INT NULL,
  LegacyLookupCode VARCHAR(150) NULL,
  FedTaxNo VARCHAR(20) NULL,
  StateTaxNo VARCHAR(20) NULL,
  CreditLimit DECIMAL(12,2) NULL,
  Comments TEXT NULL,
  CONSTRAINT fk_vendor_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE
  ,CONSTRAINT fk_vendor_lookup FOREIGN KEY (VendorLookupCodeID) REFERENCES vendor_lookup_codes(VendorLookupCodeID)
      ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Vendor role profile (1:1 with entities when the entity is a vendor). Holds vendor-specific defaults and legacy IDs.';

-- Lookup for employee employment status values referenced by employee_accounts.EmployeeStatusID.
CREATE TABLE IF NOT EXISTS employee_statuses (
  EmployeeStatusID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  StatusName VARCHAR(80) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup for employee employment status values referenced by employee_accounts.EmployeeStatusID.';

-- Employee role profile (1:1 with entities when the entity is an employee). Holds HR/payroll-ish defaults and status FK.
CREATE TABLE IF NOT EXISTS employee_accounts (
  EntityID INT NOT NULL PRIMARY KEY,
  LegacyEmployeeID VARCHAR(30) UNIQUE,
  PayTypeID INT NULL,
  Supervisor VARCHAR(50) NULL,
  SearchKey VARCHAR(50) NULL,
  SSNNo VARCHAR(30) NULL,
  HireDate DATE NULL,
  TerminationDate DATE NULL,
  Pay DECIMAL(12,2) NULL,
  EmployeeStatusID INT NULL,
  Comments TEXT NULL,
  Active TINYINT(1) NOT NULL DEFAULT 1,
  CONSTRAINT fk_emp_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE,
  CONSTRAINT fk_emp_status FOREIGN KEY (EmployeeStatusID) REFERENCES employee_statuses(EmployeeStatusID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Employee role profile (1:1 with entities when the entity is an employee). Holds HR/payroll-ish defaults and status FK.';

-- Sublet provider role profile (1:1 with entities). Minimal today; extend later for rates/terms.
CREATE TABLE IF NOT EXISTS sublet_accounts (
  EntityID INT NOT NULL PRIMARY KEY,
  Notes TEXT NULL,
  CONSTRAINT fk_sublet_entity FOREIGN KEY (EntityID) REFERENCES entities(EntityID) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Sublet provider role profile (1:1 with entities). Minimal today; extend later for rates/terms.';

-- Timeclock tables
CREATE TABLE IF NOT EXISTS time_actions (
  action_id      SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  action_code    VARCHAR(32)        NOT NULL,
  action_name    VARCHAR(64)        NOT NULL,
  is_active      TINYINT(1)         NOT NULL DEFAULT 1,
  sort_order     SMALLINT UNSIGNED  NOT NULL DEFAULT 0,
  PRIMARY KEY (action_id),
  UNIQUE KEY uq_time_actions_code (action_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup of timeclock actions such as CLOCK_IN, CLOCK_OUT, BREAK_START, etc.';

CREATE TABLE IF NOT EXISTS time_entry_types (
  entry_type_id  SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
  type_code      VARCHAR(32)        NOT NULL,
  type_name      VARCHAR(64)        NOT NULL,
  is_active      TINYINT(1)         NOT NULL DEFAULT 1,
  sort_order     SMALLINT UNSIGNED  NOT NULL DEFAULT 0,
  PRIMARY KEY (entry_type_id),
  UNIQUE KEY uq_time_entry_types_code (type_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Lookup describing why/how a time entry was created (terminal, admin, import, etc).';

CREATE TABLE IF NOT EXISTS employee_work_sessions (
  work_session_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  entity_id              INT             NOT NULL,
  opened_at              DATETIME        NOT NULL,
  closed_at              DATETIME        NULL,
  created_at             DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by_entity_id   INT             NULL,
  voided_at              DATETIME        NULL,
  voided_by_entity_id    INT             NULL,
  void_reason            VARCHAR(255)    NULL,
  PRIMARY KEY (work_session_id),
  KEY idx_sessions_entity_opened (entity_id, opened_at),
  KEY idx_sessions_opened (opened_at),
  CONSTRAINT fk_sessions_entity FOREIGN KEY (entity_id) REFERENCES entities(EntityID),
  CONSTRAINT fk_sessions_created_by FOREIGN KEY (created_by_entity_id) REFERENCES entities(EntityID),
  CONSTRAINT fk_sessions_voided_by FOREIGN KEY (voided_by_entity_id) REFERENCES entities(EntityID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Optional grouping of time events into a shift/work session.';
CREATE TABLE IF NOT EXISTS employee_time_events (
  time_event_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  entity_id             INT             NOT NULL,
  action_id             SMALLINT UNSIGNED NOT NULL,
  event_at              DATETIME        NOT NULL,
  entry_type_id         SMALLINT UNSIGNED NOT NULL,
  work_session_id       BIGINT UNSIGNED NULL,
  minutes               INT NULL,
  note                  VARCHAR(255) NULL,
  created_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by_entity_id  INT             NULL,
  voided_at             DATETIME        NULL,
  voided_by_entity_id   INT             NULL,
  void_reason           VARCHAR(255)    NULL,
  PRIMARY KEY (time_event_id),
  KEY idx_time_events_entity_at (entity_id, event_at),
  KEY idx_time_events_at (event_at),
  KEY idx_time_events_action_at (action_id, event_at),
  KEY idx_time_events_session (work_session_id),
  CONSTRAINT fk_time_events_action FOREIGN KEY (action_id) REFERENCES time_actions(action_id),
  CONSTRAINT fk_time_events_entry_type FOREIGN KEY (entry_type_id) REFERENCES time_entry_types(entry_type_id),
  CONSTRAINT fk_time_events_session FOREIGN KEY (work_session_id) REFERENCES employee_work_sessions(work_session_id),
  CONSTRAINT fk_time_events_entity FOREIGN KEY (entity_id) REFERENCES entities(EntityID),
  CONSTRAINT fk_time_events_created_by FOREIGN KEY (created_by_entity_id) REFERENCES entities(EntityID),
  CONSTRAINT fk_time_events_voided_by FOREIGN KEY (voided_by_entity_id) REFERENCES entities(EntityID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Event/punch log for employee timeclock actions.';

SET FOREIGN_KEY_CHECKS=1;
SQL;

$dropSql = <<<SQL
SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS employee_time_events;
DROP TABLE IF EXISTS employee_work_sessions;
DROP TABLE IF EXISTS time_entry_types;
DROP TABLE IF EXISTS time_actions;
DROP TABLE IF EXISTS sublet_accounts;
DROP TABLE IF EXISTS employee_accounts;
DROP TABLE IF EXISTS employee_statuses;
DROP TABLE IF EXISTS vendor_accounts;
DROP TABLE IF EXISTS vendor_lookup_codes;
DROP TABLE IF EXISTS customer_tax_info;
DROP TABLE IF EXISTS customer_billing;
DROP TABLE IF EXISTS pricing_plans;
DROP TABLE IF EXISTS customer_types;
DROP TABLE IF EXISTS customer_accounts;
DROP TABLE IF EXISTS entity_addresses;
DROP TABLE IF EXISTS entity_emails;
DROP TABLE IF EXISTS entity_phones;
DROP TABLE IF EXISTS address_types;
DROP TABLE IF EXISTS email_types;
DROP TABLE IF EXISTS phone_types;
DROP TABLE IF EXISTS entity_role_map;
DROP TABLE IF EXISTS entity_roles;
DROP TABLE IF EXISTS entity_organizations;
DROP TABLE IF EXISTS entity_persons;
DROP TABLE IF EXISTS entities;
DROP TABLE IF EXISTS org_types;
DROP TABLE IF EXISTS entity_kinds;
SET FOREIGN_KEY_CHECKS=1;
SQL;

// -------------------------
// Seed + lookup helpers
// -------------------------
function ensure_seed_data(mysqli $dst, bool $dryRun): void
{
  if ($dryRun) return;

  // kinds
  q($dst, "INSERT IGNORE INTO entity_kinds (EntityKindID, Code, Description) VALUES (1,'PERSON','Person'),(2,'ORG','Organization')");

  // org types
  q($dst, "INSERT IGNORE INTO org_types (Code, Description) VALUES
        ('BUSINESS','Business'),
        ('GOVERNMENT','Government'),
        ('FINANCIAL','Financial / Card / Bank'),
        ('OTHER','Other')");

  // roles
  q($dst, "INSERT IGNORE INTO entity_roles (Code, Description) VALUES
        ('CUSTOMER','Customer'),
        ('VENDOR','Vendor'),
        ('EMPLOYEE','Employee'),
        ('SUBLET_PROVIDER','Sublet Provider')");

  // contact types
  q($dst, "INSERT IGNORE INTO phone_types (PhoneTypeName) VALUES ('main'),('mobile'),('work'),('home'),('fax')");
  q($dst, "INSERT IGNORE INTO email_types (EmailTypeName) VALUES ('main'),('work'),('billing'),('personal')");
  q($dst, "INSERT IGNORE INTO address_types (AddressTypeName) VALUES ('billing'),('shipping'),('service'),('mailing'),('main'),('home'),('work')");

  // customer type enum replacement
  q($dst, "INSERT IGNORE INTO customer_types (CustomerTypeName) VALUES ('Charge Account'),('Retail'),('Commercial'),('Wholesale')");

  // starter pricing plans (auto-extended by migration if needed)
  q($dst, "INSERT IGNORE INTO pricing_plans (Code, Multiplier, Description) VALUES
        ('D',1.0000,'Default D'),
        ('C',1.0000,'Default C'),
        ('B',1.0000,'Default B'),
        ('A',1.0000,'Default A')");

  // timeclock lookups
  q($dst, "INSERT IGNORE INTO time_actions (action_code, action_name, sort_order) VALUES
        ('CLOCK_IN', 'Clock In', 10),
        ('CLOCK_OUT', 'Clock Out', 20),
        ('BREAK_START', 'Break Start', 30),
        ('BREAK_END', 'Break End', 40),
        ('MEAL_START', 'Meal Start', 50),
        ('MEAL_END', 'Meal End', 60),
        ('ADJUSTMENT', 'Adjustment', 70),
        ('PTO', 'PTO', 80)");

  q($dst, "INSERT IGNORE INTO time_entry_types (type_code, type_name, sort_order) VALUES
        ('TERMINAL',  'Terminal/Timeclock', 10),
        ('MOBILE',    'Mobile',             20),
        ('ADMIN',     'Admin Entry',        30),
        ('IMPORT',    'Imported',           40),
        ('SYSTEM',    'System Generated',   50),
        ('PTO',       'PTO Entry',          60)");
}

function ensure_timeclock_schema(mysqli $dst, bool $dryRun, string $schema): void
{
  $ddl = [
    "CREATE TABLE IF NOT EXISTS time_actions (
            action_id      SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
            action_code    VARCHAR(32)        NOT NULL,
            action_name    VARCHAR(64)        NOT NULL,
            is_active      TINYINT(1)         NOT NULL DEFAULT 1,
            sort_order     SMALLINT UNSIGNED  NOT NULL DEFAULT 0,
            PRIMARY KEY (action_id),
            UNIQUE KEY uq_time_actions_code (action_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS time_entry_types (
            entry_type_id  SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
            type_code      VARCHAR(32)        NOT NULL,
            type_name      VARCHAR(64)        NOT NULL,
            is_active      TINYINT(1)         NOT NULL DEFAULT 1,
            sort_order     SMALLINT UNSIGNED  NOT NULL DEFAULT 0,
            PRIMARY KEY (entry_type_id),
            UNIQUE KEY uq_time_entry_types_code (type_code)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS employee_work_sessions (
            work_session_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            entity_id              INT UNSIGNED    NOT NULL,
            opened_at              DATETIME        NOT NULL,
            closed_at              DATETIME        NULL,
            created_at             DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by_entity_id   INT UNSIGNED    NULL,
            voided_at              DATETIME        NULL,
            voided_by_entity_id    INT UNSIGNED    NULL,
            void_reason            VARCHAR(255)    NULL,
            PRIMARY KEY (work_session_id),
            KEY idx_sessions_entity_opened (entity_id, opened_at),
            KEY idx_sessions_opened (opened_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    "CREATE TABLE IF NOT EXISTS employee_time_events (
            time_event_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
            entity_id             INT UNSIGNED    NOT NULL,
            action_id             SMALLINT UNSIGNED NOT NULL,
            event_at              DATETIME        NOT NULL,
            entry_type_id         SMALLINT UNSIGNED NOT NULL,
            work_session_id       BIGINT UNSIGNED NULL,
            minutes               INT NULL,
            note                  VARCHAR(255) NULL,
            created_at            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_by_entity_id  INT UNSIGNED    NULL,
            voided_at             DATETIME        NULL,
            voided_by_entity_id   INT UNSIGNED    NULL,
            void_reason           VARCHAR(255)    NULL,
            PRIMARY KEY (time_event_id),
            KEY idx_time_events_entity_at (entity_id, event_at),
            KEY idx_time_events_at (event_at),
            KEY idx_time_events_action_at (action_id, event_at),
            KEY idx_time_events_session (work_session_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
  ];

  foreach ($ddl as $sql) {
    if ($dryRun) continue;
    q($dst, $sql);
  }

  $hasSessions = table_exists($dst, $schema, 'employee_work_sessions');
  $hasEvents = table_exists($dst, $schema, 'employee_time_events');

  if ($hasSessions) {
    ensure_index($dst, $dryRun, $schema, 'employee_work_sessions', 'idx_sessions_entity_opened', "INDEX idx_sessions_entity_opened (entity_id, opened_at)");
    ensure_index($dst, $dryRun, $schema, 'employee_work_sessions', 'idx_sessions_opened', "INDEX idx_sessions_opened (opened_at)");

    ensure_fk($dst, $dryRun, $schema, 'employee_work_sessions', 'fk_sessions_entity', "FOREIGN KEY (entity_id) REFERENCES entities(EntityID)");
    ensure_fk($dst, $dryRun, $schema, 'employee_work_sessions', 'fk_sessions_created_by', "FOREIGN KEY (created_by_entity_id) REFERENCES entities(EntityID)");
    ensure_fk($dst, $dryRun, $schema, 'employee_work_sessions', 'fk_sessions_voided_by', "FOREIGN KEY (voided_by_entity_id) REFERENCES entities(EntityID)");
  }

  if ($hasEvents) {
    ensure_index($dst, $dryRun, $schema, 'employee_time_events', 'idx_time_events_entity_at', "INDEX idx_time_events_entity_at (entity_id, event_at)");
    ensure_index($dst, $dryRun, $schema, 'employee_time_events', 'idx_time_events_at', "INDEX idx_time_events_at (event_at)");
    ensure_index($dst, $dryRun, $schema, 'employee_time_events', 'idx_time_events_action_at', "INDEX idx_time_events_action_at (action_id, event_at)");
    ensure_index($dst, $dryRun, $schema, 'employee_time_events', 'idx_time_events_session', "INDEX idx_time_events_session (work_session_id)");

    ensure_fk($dst, $dryRun, $schema, 'employee_time_events', 'fk_time_events_action', "FOREIGN KEY (action_id) REFERENCES time_actions(action_id)");
    ensure_fk($dst, $dryRun, $schema, 'employee_time_events', 'fk_time_events_entry_type', "FOREIGN KEY (entry_type_id) REFERENCES time_entry_types(entry_type_id)");
    ensure_fk($dst, $dryRun, $schema, 'employee_time_events', 'fk_time_events_session', "FOREIGN KEY (work_session_id) REFERENCES employee_work_sessions(work_session_id)");
    ensure_fk($dst, $dryRun, $schema, 'employee_time_events', 'fk_time_events_entity', "FOREIGN KEY (entity_id) REFERENCES entities(EntityID)");
    ensure_fk($dst, $dryRun, $schema, 'employee_time_events', 'fk_time_events_created_by', "FOREIGN KEY (created_by_entity_id) REFERENCES entities(EntityID)");
    ensure_fk($dst, $dryRun, $schema, 'employee_time_events', 'fk_time_events_voided_by', "FOREIGN KEY (voided_by_entity_id) REFERENCES entities(EntityID)");
  }
}

function role_id(mysqli $dst, string $code): int
{
  $stmt = $dst->prepare("SELECT RoleID FROM entity_roles WHERE Code=? LIMIT 1");
  $stmt->bind_param("s", $code);
  $stmt->execute();
  $stmt->bind_result($id);
  if ($stmt->fetch()) {
    $stmt->close();
    return intval($id);
  }
  $stmt->close();
  throw new RuntimeException("Role not seeded: $code");
}

function org_type_id(mysqli $dst, string $code): int
{
  $stmt = $dst->prepare("SELECT OrgTypeID FROM org_types WHERE Code=? LIMIT 1");
  $stmt->bind_param("s", $code);
  $stmt->execute();
  $stmt->bind_result($id);
  if ($stmt->fetch()) {
    $stmt->close();
    return intval($id);
  }
  $stmt->close();
  throw new RuntimeException("Org type not seeded: $code");
}

function get_lookup_id(mysqli $dst, string $table, string $idCol, string $nameCol, string $value): int
{
  $value = trim($value);
  $stmt = $dst->prepare("SELECT $idCol FROM $table WHERE $nameCol=? LIMIT 1");
  $stmt->bind_param("s", $value);
  $stmt->execute();
  $stmt->bind_result($id);
  if ($stmt->fetch()) {
    $stmt->close();
    return intval($id);
  }
  $stmt->close();
  $stmt = $dst->prepare("INSERT INTO $table ($nameCol) VALUES (?)");
  $stmt->bind_param("s", $value);
  $stmt->execute();
  $newId = intval($stmt->insert_id);
  $stmt->close();
  return $newId;
}


/**
 * Normalize legacy vendor lookup codes for mapping (trim, upper, remove punctuation, collapse spaces).
 */
function norm_vendor_lookup(string $s): string
{
  $s = strtoupper(trim($s));
  $s = str_replace(["'", '"'], "", $s);
  $s = preg_replace('/[^A-Z0-9]+/', ' ', $s);
  $s = preg_replace('/\s+/', ' ', $s);
  return trim($s);
}

/**
 * Map legacy tblVendors.LookupCode to a canonical vendor_lookup_codes.Code.
 * Based on your conversion table (shop_db_1.tblVendors.LookupCode -> vendor_lookup_codes.Code).
 */
function load_vendor_lookup_descriptions(string $schemaPath = '/data-source/schema_summary.json'): array
{
  static $cache = null;
  if ($cache !== null) return $cache;

  if (!is_readable($schemaPath)) return $cache = [];
  $json = @file_get_contents($schemaPath);
  if ($json === false) return $cache = [];
  $data = json_decode($json, true);
  if (!is_array($data)) return $cache = [];

  // Try a few likely shapes to extract seed descriptions
  $table = $data['vendor_lookup_codes'] ?? ($data['tables']['vendor_lookup_codes'] ?? ($data['tables']['vendor_lookup_codes']['seed'] ?? null));
  if ($table === null && isset($data['tables'])) {
    // maybe the entry itself is an array of rows under tables
    $table = $data['tables']['vendor_lookup_codes'] ?? null;
  }

  $descs = [];
  $rows = [];
  if (is_array($table)) {
    if (isset($table['seed']) && is_array($table['seed'])) {
      $rows = $table['seed'];
    } else {
      $rows = $table;
    }
  }

  foreach ($rows as $row) {
    if (is_array($row) && isset($row['Code'])) {
      $code = strtoupper(trim((string)$row['Code']));
      $desc = trim((string)($row['Description'] ?? ''));
      if ($code !== '') {
        $descs[$code] = $desc !== '' ? $desc : $code;
      }
    }
  }

  return $cache = $descs;
}

/**
 * Map legacy tblVendors.LookupCode to a canonical vendor_lookup_codes.Code.
 * Synonyms originate from the provided conversion map (old values -> new code key).
 * Descriptions are loaded from /data-source/schema_summary.json when available.
 */
function map_vendor_lookup(?string $legacy): array
{
  $legacy = $legacy ?? '';
  $n = norm_vendor_lookup($legacy);

  $synonymMap = [
    'TOWING'            => ['TOWING', 'WRECKER', 'WRECKER'],
    'OFFICE_SUPPLIES'   => ['OFFICE SUPPLIES', 'OFFICE'],
    'SHOP_SUPPLIES'     => ['SUPPLIES', 'SHOP', 'OFFICE-SHOP SUPPLIES'],
    'PARTS'             => ['PARTS', 'PART', "PART'S", 'LIGHTS', 'OIL', 'TIRES', 'TIRE', "TIRE'S", 'BUS PARTS', 'GSE', 'HYD.PARTS', 'ALTERNATOR', 'METAL', 'PAINT', 'BODY'],
    'TOOLS'             => ['TOOLS'],
    'FREIGHT'           => ['FREIGHT', 'SHIPPING'],
    'UTILITIES'         => ['SERVICES', 'UTILITIES'],
    'COMMUNICATIONS'    => ['PHONE', 'ENTERTAINMENT'],
    'RENT'              => ['RENT'],
    'BANKING'           => ['BANK', 'BANKING'],
    'CREDIT_CARDS'      => ['CREDIT CARDS'],
    'TAXES'             => ['TAXES'],
    'GAS_FUEL'          => ['GAS'],
    'MEALS'             => ['MEALS'],
    'CLOTHING'          => ['CLOTHING'],
    'OFFICE_EQUIPMENT'  => ['COMPUTER'],
    'CHARITY'           => ['CHARITY'],
    'MEDICAL'           => ['MEDICAL'],
    'FLOWERS'           => ['FLOWERS'],
  ];

  // Add self as synonym to allow already normalized codes to map through.
  foreach ($synonymMap as $code => &$syns) {
    $syns[] = $code;
  }
  unset($syns);

  $descs = load_vendor_lookup_descriptions();

  foreach ($synonymMap as $code => $syns) {
    foreach ($syns as $syn) {
      if ($n === norm_vendor_lookup($syn)) {
        $desc = $descs[$code] ?? str_replace('_', ' ', ucwords(strtolower($code), '_'));
        return ['code' => $code, 'desc' => $desc, 'legacy_norm' => $n];
      }
    }
  }

  return ['code' => 'UNCLASSIFIED', 'desc' => 'Unclassified', 'legacy_norm' => $n];
}

function getVendorLookupCodeId(mysqli $dst, string $code, string $desc): int
{
  $ins = $dst->prepare("INSERT IGNORE INTO vendor_lookup_codes (Code, Description) VALUES (?, ?)");
  $ins->bind_param("ss", $code, $desc);
  $ins->execute();
  $ins->close();

  $sel = $dst->prepare("SELECT VendorLookupCodeID FROM vendor_lookup_codes WHERE Code = ?");
  $sel->bind_param("s", $code);
  $sel->execute();
  $res = $sel->get_result();
  $row = $res->fetch_assoc();
  $sel->close();
  return (int)$row['VendorLookupCodeID'];
}


function getPhoneTypeId(mysqli $dst, string $name): int
{
  return get_lookup_id($dst, 'phone_types', 'PhoneTypeID', 'PhoneTypeName', strtolower(trim($name ?: 'main')));
}
function getEmailTypeId(mysqli $dst, string $name): int
{
  return get_lookup_id($dst, 'email_types', 'EmailTypeID', 'EmailTypeName', strtolower(trim($name ?: 'main')));
}
function getAddressTypeId(mysqli $dst, string $name): int
{
  return get_lookup_id($dst, 'address_types', 'AddressTypeID', 'AddressTypeName', strtolower(trim($name ?: 'main')));
}
function getCustomerTypeId(mysqli $dst, string $name): int
{
  return get_lookup_id($dst, 'customer_types', 'CustomerTypeID', 'CustomerTypeName', trim($name));
}

function getPricingPlanId(mysqli $dst, string $code): int
{
  $code = trim($code);
  if ($code === '') $code = 'D';
  $stmt = $dst->prepare("SELECT PricingPlanID FROM pricing_plans WHERE Code=? LIMIT 1");
  $stmt->bind_param("s", $code);
  $stmt->execute();
  $stmt->bind_result($id);
  if ($stmt->fetch()) {
    $stmt->close();
    return intval($id);
  }
  $stmt->close();

  // auto-create with default multiplier 1.0000
  $desc = $code;
  $stmt = $dst->prepare("INSERT INTO pricing_plans (Code, Multiplier, Description) VALUES (?, 1.0000, ?)");
  $stmt->bind_param("ss", $code, $desc);
  $stmt->execute();
  $newId = intval($stmt->insert_id);
  $stmt->close();
  return $newId;
}

function time_action_id(mysqli $dst, string $code): int
{
  $code = strtoupper(trim($code));
  $stmt = $dst->prepare("SELECT action_id FROM time_actions WHERE action_code = ? LIMIT 1");
  $stmt->bind_param("s", $code);
  $stmt->execute();
  $stmt->bind_result($id);
  if ($stmt->fetch()) {
    $stmt->close();
    return intval($id);
  }
  $stmt->close();
  throw new RuntimeException("Time action not seeded: $code");
}

function entry_type_id(mysqli $dst, string $code): int
{
  $code = strtoupper(trim($code));
  $stmt = $dst->prepare("SELECT entry_type_id FROM time_entry_types WHERE type_code = ? LIMIT 1");
  $stmt->bind_param("s", $code);
  $stmt->execute();
  $stmt->bind_result($id);
  if ($stmt->fetch()) {
    $stmt->close();
    return intval($id);
  }
  $stmt->close();
  throw new RuntimeException("Time entry type not seeded: $code");
}

function add_phone(mysqli $dst, bool $dryRun, int $entityID, string $type, ?string $num, bool $primary = false): void
{
  $num = trim((string)$num);
  if ($num === '') return;
  if ($dryRun) return;
  $typeId = getPhoneTypeId($dst, $type);
  $prim = $primary ? 1 : 0;
  $stmt = $dst->prepare("INSERT INTO entity_phones (EntityID, PhoneTypeID, PhoneNumber, IsPrimary) VALUES (?,?,?,?)");
  $stmt->bind_param("iisi", $entityID, $typeId, $num, $prim);
  $stmt->execute();
  $stmt->close();
}

function add_email(mysqli $dst, bool $dryRun, int $entityID, string $type, ?string $email, bool $primary = false): void
{
  $email = trim((string)$email);
  if ($email === '') return;
  if ($dryRun) return;
  $typeId = getEmailTypeId($dst, $type);
  $prim = $primary ? 1 : 0;
  $stmt = $dst->prepare("INSERT INTO entity_emails (EntityID, EmailTypeID, Email, IsPrimary) VALUES (?,?,?,?)");
  $stmt->bind_param("iisi", $entityID, $typeId, $email, $prim);
  $stmt->execute();
  $stmt->close();
}

function add_address(mysqli $dst, bool $dryRun, int $entityID, string $type, ?string $a1, ?string $a2, ?string $city, ?string $state, ?string $postal, ?string $country = 'US', bool $primary = false): void
{
  $a1 = trim((string)$a1);
  $city = trim((string)$city);
  $state = trim((string)$state);
  $postal = trim((string)$postal);
  if ($a1 === '' && $city === '' && $state === '' && $postal === '') return;
  if ($dryRun) return;

  $typeId = getAddressTypeId($dst, $type);
  $prim = $primary ? 1 : 0;
  $stmt = $dst->prepare("INSERT IGNORE INTO entity_addresses (EntityID, AddressTypeID, Address1, Address2, City, State, Postal, Country, IsPrimary)
                           VALUES (?,?,?,?,?,?,?,?,?)");
  $stmt->bind_param("iissssssi", $entityID, $typeId, $a1, $a2, $city, $state, $postal, $country, $prim);
  $stmt->execute();
  $stmt->close();
}

function normalize_datetime(?string $dt): ?string
{
  $dt = trim((string)$dt);
  if ($dt === '' || $dt === '0000-00-00' || $dt === '0000-00-00 00:00:00') return null;
  try {
    $d = new DateTime($dt);
    return $d->format('Y-m-d H:i:s');
  } catch (Throwable $e) {
    return null;
  }
}

function add_minutes_to_datetime(string $dt, int $minutes): string
{
  try {
    $d = new DateTime($dt);
    $d->modify(($minutes >= 0 ? '+' : '') . $minutes . ' minutes');
    return $d->format('Y-m-d H:i:s');
  } catch (Throwable $e) {
    return $dt;
  }
}

function align_clock_with_trans_date(?string $clock, ?string $transDate): ?string
{
  if ($clock === null || $transDate === null) return $clock;
  try {
    $clockDt = new DateTime($clock);
    $year = intval($clockDt->format('Y'));
    if ($year < 1970) {
      $datePart = (new DateTime($transDate))->format('Y-m-d');
      return $datePart . ' ' . $clockDt->format('H:i:s');
    }
  } catch (Throwable $e) {
    return $clock;
  }
  return $clock;
}

function ensure_clock_span(?string $clockIn, ?string $clockOut, ?string $transDate, int $defaultMinutes = 480): array
{
  $hasIn = $clockIn !== null;
  $hasOut = $clockOut !== null;

  if (!$hasIn && !$hasOut) {
    if ($transDate === null) return [null, null];
    $datePart = (new DateTime($transDate))->format('Y-m-d');
    $clockIn = $datePart . ' 08:00:00';
    $clockOut = add_minutes_to_datetime($clockIn, $defaultMinutes);
  } elseif ($hasIn && !$hasOut) {
    $clockOut = add_minutes_to_datetime($clockIn, $defaultMinutes);
  } elseif (!$hasIn && $hasOut) {
    $clockIn = add_minutes_to_datetime($clockOut, -$defaultMinutes);
  }

  $inTs = $clockIn ? strtotime($clockIn) : false;
  $outTs = $clockOut ? strtotime($clockOut) : false;
  if ($inTs !== false && $outTs !== false && $outTs <= $inTs) {
    $clockOut = date('Y-m-d H:i:s', $inTs + ($defaultMinutes * 60));
  }

  return [$clockIn, $clockOut];
}

function map_trans_type_to_action(string $legacyType, bool $isOutContext = false): string
{
  $t = strtoupper(trim($legacyType));
  $patterns = [
    'CLOCK IN'   => 'CLOCK_IN',
    'CLOCKIN'    => 'CLOCK_IN',
    'CLOCK OUT'  => 'CLOCK_OUT',
    'CLOCKOUT'   => 'CLOCK_OUT',
    'BREAK START' => 'BREAK_START',
    'BREAK END'  => 'BREAK_END',
    'LUNCH OUT'  => 'MEAL_START',
    'LUNCH IN'   => 'MEAL_END',
    'MEAL OUT'   => 'MEAL_START',
    'MEAL IN'    => 'MEAL_END',
    'PTO'        => 'PTO',
  ];
  foreach ($patterns as $needle => $action) {
    if (str_contains($t, $needle)) return $action;
  }

  if (str_contains($t, 'BREAK')) return $isOutContext ? 'BREAK_END' : 'BREAK_START';
  if (str_contains($t, 'MEAL') || str_contains($t, 'LUNCH')) return $isOutContext ? 'MEAL_END' : 'MEAL_START';
  if (str_contains($t, 'OUT')) return 'CLOCK_OUT';
  if (str_contains($t, 'IN')) return 'CLOCK_IN';

  return $isOutContext ? 'CLOCK_OUT' : 'CLOCK_IN';
}

function end_action_for_start(string $startAction): string
{
  return match ($startAction) {
    'BREAK_START' => 'BREAK_END',
    'MEAL_START'  => 'MEAL_END',
    'PTO'         => 'PTO',
    default       => 'CLOCK_OUT',
  };
}

function get_or_create_entity_person(mysqli $dst, bool $dryRun, array &$personMap, string $first, ?string $middle, string $last, string $displayName, ?string $addrKey): int
{
  $key = normalize_key($first . '|' . $last . '|' . (string)$addrKey);
  if ($key !== '' && isset($personMap[$key])) return $personMap[$key];
  if ($dryRun) return -1;

  q($dst, "INSERT INTO entities (EntityKindID, DisplayName) VALUES (1,'" . $dst->real_escape_string($displayName) . "')");
  $eid = intval($dst->insert_id);
  $stmt = $dst->prepare("INSERT INTO entity_persons (EntityID, FirstName, MiddleName, LastName) VALUES (?,?,?,?)");
  $stmt->bind_param("isss", $eid, $first, $middle, $last);
  $stmt->execute();
  $stmt->close();

  if ($key !== '') $personMap[$key] = $eid;
  return $eid;
}

function get_or_create_entity_org(mysqli $dst, bool $dryRun, array &$orgMap, string $legalName, int $orgTypeID, string $displayName): int
{
  $key = normalize_key($legalName);
  if ($key !== '' && isset($orgMap[$key])) return $orgMap[$key];
  if ($dryRun) return -1;

  q($dst, "INSERT INTO entities (EntityKindID, DisplayName) VALUES (2,'" . $dst->real_escape_string($displayName) . "')");
  $eid = intval($dst->insert_id);
  $stmt = $dst->prepare("INSERT INTO entity_organizations (EntityID, OrgTypeID, LegalName, DBAName, TaxID) VALUES (?,?,?,?,NULL)");
  $dba = null;
  $stmt->bind_param("iiss", $eid, $orgTypeID, $legalName, $dba);
  $stmt->execute();
  $stmt->close();

  if ($key !== '') $orgMap[$key] = $eid;
  return $eid;
}

function assign_role(mysqli $dst, bool $dryRun, int $entityID, string $roleCode): void
{
  if ($dryRun) return;
  $rid = role_id($dst, $roleCode);
  $stmt = $dst->prepare("INSERT IGNORE INTO entity_role_map (EntityID, RoleID) VALUES (?,?)");
  $stmt->bind_param("ii", $entityID, $rid);
  $stmt->execute();
  $stmt->close();
}

function upsert_customer(mysqli $dst, bool $dryRun, int $entityID, ?int $legacyCustomerID, ?string $internalComments, ?string $typeName, $creditLim, $checkApp, $chargeApp, ?string $defaultPricing, $taxExempt, ?string $resaleNo): void
{
  if ($dryRun) return;

  // customer_accounts
  $stmt = $dst->prepare("INSERT IGNORE INTO customer_accounts (EntityID, LegacyCustomerID, InternalComments) VALUES (?,?,?)");
  $stmt->bind_param("iis", $entityID, $legacyCustomerID, $internalComments);
  $stmt->execute();
  $stmt->close();

  // billing
  $custTypeID = null;
  if ($typeName !== null && trim($typeName) !== '') $custTypeID = getCustomerTypeId($dst, trim($typeName));

  $cl = null;
  if ($creditLim !== null && $creditLim !== '' && is_numeric($creditLim)) $cl = floatval($creditLim);

  $acceptChecks = ($checkApp !== null) ? (intval($checkApp) ? 1 : 0) : 1;
  $acceptCharge = ($chargeApp !== null) ? (intval($chargeApp) ? 1 : 0) : 1;

  $pricingPlanID = null;
  if ($defaultPricing !== null && trim($defaultPricing) !== '') $pricingPlanID = getPricingPlanId($dst, trim($defaultPricing));

  $stmt = $dst->prepare("INSERT IGNORE INTO customer_billing (EntityID, CustomerTypeID, CreditLimit, AcceptChecks, AcceptCharge, DefaultPricingPlanID, Notes)
                           VALUES (?,?,?,?,?,?,NULL)");
  // bind: i i d i i i
  $stmt->bind_param("iidiii", $entityID, $custTypeID, $cl, $acceptChecks, $acceptCharge, $pricingPlanID);
  $stmt->execute();
  $stmt->close();

  // tax
  $tx = (intval($taxExempt) ? 1 : 0);
  $stmt = $dst->prepare("INSERT IGNORE INTO customer_tax_info (EntityID, TaxExempt, ResaleNo) VALUES (?,?,?)");
  $stmt->bind_param("iis", $entityID, $tx, $resaleNo);
  $stmt->execute();
  $stmt->close();
}

function upsert_vendor(mysqli $dst, bool $dryRun, int $entityID, ?int $legacyVendorID, $paytermsID, $acctID, $vendorLookupCodeID, ?string $legacyLookupCode, ?string $fedTaxNo, ?string $stateTaxNo, $creditLimit, ?string $comments): void
{
  if ($dryRun) return;
  $cl = null;
  if ($creditLimit !== null && $creditLimit !== '' && is_numeric($creditLimit)) $cl = floatval($creditLimit);
  $stmt = $dst->prepare("INSERT IGNORE INTO vendor_accounts (EntityID, LegacyVendorID, PaytermsID, AcctID, VendorLookupCodeID, LegacyLookupCode, FedTaxNo, StateTaxNo, CreditLimit, Comments)
                           VALUES (?,?,?,?,?,?,?,?,?,?)");
  $stmt->bind_param("iiiiisssds", $entityID, $legacyVendorID, $paytermsID, $acctID, $vendorLookupCodeID, $legacyLookupCode, $fedTaxNo, $stateTaxNo, $cl, $comments);
  $stmt->execute();
  $stmt->close();
}

function getEmployeeStatusId(mysqli $dst, ?string $status): ?int
{
  $status = trim((string)$status);
  if ($status === '') return null;
  return get_lookup_id($dst, 'employee_statuses', 'EmployeeStatusID', 'StatusName', $status);
}

function upsert_employee(mysqli $dst, bool $dryRun, int $entityID, ?string $legacyEmployeeID, $payTypeID, ?string $supervisor, ?string $searchKey, ?string $ssnNo, ?string $hireDate, ?string $termDate, $pay, ?string $currentStatus, ?string $comments, $active): void
{
  if ($dryRun) return;
  $statusId = getEmployeeStatusId($dst, $currentStatus);
  $p = null;
  if ($pay !== null && $pay !== '' && is_numeric($pay)) $p = floatval($pay);
  $act = (intval($active) ? 1 : 0);

  $stmt = $dst->prepare("INSERT IGNORE INTO employee_accounts (EntityID, LegacyEmployeeID, PayTypeID, Supervisor, SearchKey, SSNNo, HireDate, TerminationDate, Pay, EmployeeStatusID, Comments, Active)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
  $stmt->bind_param("isisssssdisi", $entityID, $legacyEmployeeID, $payTypeID, $supervisor, $searchKey, $ssnNo, $hireDate, $termDate, $p, $statusId, $comments, $act);
  $stmt->execute();
  $stmt->close();
}

function upsert_sublet_provider(mysqli $dst, bool $dryRun, int $entityID): void
{
  if ($dryRun) return;
  $stmt = $dst->prepare("INSERT IGNORE INTO sublet_accounts (EntityID, Notes) VALUES (?,NULL)");
  $stmt->bind_param("i", $entityID);
  $stmt->execute();
  $stmt->close();
}

function employee_entity_id(mysqli $dst, ?string $legacyEmployeeId): ?int
{
  $legacyEmployeeId = trim((string)$legacyEmployeeId);
  if ($legacyEmployeeId === '') return null;

  $stmt = $dst->prepare("SELECT EntityID FROM employee_accounts WHERE LegacyEmployeeID = ? LIMIT 1");
  $stmt->bind_param("s", $legacyEmployeeId);
  $stmt->execute();
  $res = $stmt->get_result();
  $row = $res ? $res->fetch_assoc() : null;
  $stmt->close();
  if ($row && isset($row['EntityID'])) return intval($row['EntityID']);
  return null;
}

function migrate_timeclock(mysqli $src, mysqli $dst, bool $dryRun): array
{
  $stats = [
    'rows' => 0,
    'events' => 0,
    'skipped_missing_employee' => 0,
    'skipped_no_timestamp' => 0,
  ];

  $transTypes = [];
  $typesRes = q($src, "SELECT TransTypeID, TransType FROM tblEmpTransType");
  while ($row = $typesRes->fetch_assoc()) {
    $id = isset($row['TransTypeID']) ? intval($row['TransTypeID']) : null;
    if ($id !== null) $transTypes[$id] = trim((string)$row['TransType']);
  }
  $typesRes->free();

  $entryTypeImportId = $dryRun ? null : entry_type_id($dst, 'IMPORT');
  $insert = null;
  $evEntityId = 0;
  $evActionId = 0;
  $evAt = '';
  $evEntryTypeId = $entryTypeImportId ?? 0;
  $evSessionId = null;
  $evMinutes = null;
  $evNote = null;

  if (!$dryRun) {
    $insert = $dst->prepare("INSERT INTO employee_time_events (entity_id, action_id, event_at, entry_type_id, work_session_id, minutes, note) VALUES (?,?,?,?,?,?,?)");
    $insert->bind_param("iisiiis", $evEntityId, $evActionId, $evAt, $evEntryTypeId, $evSessionId, $evMinutes, $evNote);
  }

  $res = q($src, "SELECT * FROM tblEmpTimeClock ORDER BY Clockin, ClockOut, TransDate, LineNo");
  while ($row = $res->fetch_assoc()) {
    $stats['rows']++;

    $entityId = employee_entity_id($dst, $row['EmployeeID'] ?? null);
    if ($entityId === null) {
      $stats['skipped_missing_employee']++;
      continue;
    }

    $transType = '';
    if (isset($row['TransTypeID'])) {
      $tid = intval($row['TransTypeID']);
      $transType = $transTypes[$tid] ?? '';
    }

    $clockIn = normalize_datetime($row['Clockin'] ?? null);
    $clockOut = normalize_datetime($row['ClockOut'] ?? null);
    $transDate = normalize_datetime($row['TransDate'] ?? null);
    $note = $transType !== '' ? $transType : null;

    $clockIn = align_clock_with_trans_date($clockIn, $transDate);
    $clockOut = align_clock_with_trans_date($clockOut, $transDate);
    [$clockIn, $clockOut] = ensure_clock_span($clockIn, $clockOut, $transDate);

    if ($clockIn !== null && $clockOut !== null) {
      $startAction = map_trans_type_to_action($transType, false);
      if ($startAction === 'CLOCK_OUT') $startAction = 'CLOCK_IN';
      if ($startAction === 'BREAK_END') $startAction = 'BREAK_START';
      if ($startAction === 'MEAL_END') $startAction = 'MEAL_START';
      $endAction = end_action_for_start($startAction);

      $minutes = null;
      $inTs = strtotime($clockIn);
      $outTs = strtotime($clockOut);
      if ($inTs !== false && $outTs !== false && $outTs > $inTs) {
        $minutes = intval(floor(($outTs - $inTs) / 60));
      }

      if ($startAction === 'PTO') {
        // Duration-style PTO entry: single row with computed minutes.
        if (!$dryRun) {
          $evEntityId = $entityId;
          $evActionId = time_action_id($dst, $startAction);
          $evAt = $clockIn;
          $evSessionId = null;
          $evMinutes = $minutes;
          $evNote = $note;
          $insert->execute();
        }
        $stats['events']++;
        continue;
      }

      if (!$dryRun) {
        $evEntityId = $entityId;
        $evActionId = time_action_id($dst, $startAction);
        $evAt = $clockIn;
        $evSessionId = null;
        $evMinutes = null;
        $evNote = $note;
        $insert->execute();

        $evEntityId = $entityId;
        $evActionId = time_action_id($dst, $endAction);
        $evAt = $clockOut;
        $evSessionId = null;
        $evMinutes = $minutes;
        $evNote = $note;
        $insert->execute();
      }
      $stats['events'] += 2;
      continue;
    }

    $eventAt = $clockIn ?? $clockOut ?? $transDate;
    if ($eventAt === null) {
      $stats['skipped_no_timestamp']++;
      continue;
    }

    $actionCode = map_trans_type_to_action($transType, $clockOut !== null);
    if (!$dryRun) {
      $evEntityId = $entityId;
      $evActionId = time_action_id($dst, $actionCode);
      $evAt = $eventAt;
      $evSessionId = null;
      $evMinutes = null;
      $evNote = $note;
      $insert->execute();
    }
    $stats['events']++;
  }
  $res->free();

  if ($insert instanceof mysqli_stmt) $insert->close();
  return $stats;
}

// -------------------------
// Main
// -------------------------
$cmd = $argv[1] ?? '';
$dryRun = in_array('--dry-run', $argv, true);

if (!in_array($cmd, ['create', 'drop', 'migrate', 'update'], true)) usage();

$dst = db_connect($DB_HOST, $DB_USER, $DB_PASS, $DB_PORT, $DST_DB);
$src = null;
if ($cmd === 'migrate') {
  $src = db_connect($DB_HOST, $DB_USER, $DB_PASS, $DB_PORT, $SRC_DB);
}

if ($cmd === 'create') {
  logmsg("Creating schema in $DST_DB ...");
  foreach (explode(";\n", $schemaSql) as $stmt) {
    $stmt = trim($stmt);
    if ($stmt === '') continue;
    q($dst, $stmt);
  }
  ensure_seed_data($dst, false);
  logmsg("Done.");
  exit(0);
}

if ($cmd === 'drop') {
  logmsg("Dropping schema objects in $DST_DB ...");
  foreach (explode(";\n", $dropSql) as $stmt) {
    $stmt = trim($stmt);
    if ($stmt === '') continue;
    q($dst, $stmt);
  }
  logmsg("Done.");
  exit(0);
}

if ($cmd === 'update') {
  logmsg("Updating schema in $DST_DB (dryRun=" . ($dryRun ? 'true' : 'false') . ") ...");
  ensure_timeclock_schema($dst, $dryRun, $DST_DB);
  ensure_seed_data($dst, $dryRun);
  logmsg("Update complete.");
  exit(0);
}

// migrate
logmsg("Migrating $SRC_DB -> $DST_DB (dryRun=" . ($dryRun ? 'true' : 'false') . ") ...");
ensure_timeclock_schema($dst, $dryRun, $DST_DB);
ensure_seed_data($dst, $dryRun);

$ORG_BUSINESS = org_type_id($dst, 'BUSINESS');
$ORG_GOV      = org_type_id($dst, 'GOVERNMENT');
$ORG_FIN      = org_type_id($dst, 'FINANCIAL');

$personMap = [];
$orgMap = [];

$stats = [
  'customers' => 0,
  'vendors' => 0,
  'employees' => 0,
  'sublet_companies' => 0,
  'time_events' => 0,
  'time_rows' => 0,
  'time_skipped_missing_employee' => 0,
  'time_skipped_no_timestamp' => 0,
];

// -------------------------
// Customers
// -------------------------
logmsg("tblCustomers -> entities/customer_* ...");
$r = q($src, "SELECT * FROM tblCustomers");
while ($row = $r->fetch_assoc()) {
  $legacyID = isset($row['CustomerID']) ? intval($row['CustomerID']) : null;
  $company  = trim((string)($row['CompanyName'] ?? ''));
  $first    = trim((string)($row['ContactFirstName'] ?? ''));
  $last     = trim((string)($row['ContactLastName'] ?? ''));
  $typeID   = trim((string)($row['TypeID'] ?? '')); // Charge Account / Retail / Commercial / Wholesale
  $display  = $company !== '' ? $company : (concat_name($first, $last) ?: ("Customer $legacyID"));

  $entityID = -1;
  if ($company !== '') {
    $isGov = ($typeID !== '' && preg_match('/gov|city|county|state|federal|school/i', $typeID));
    $orgType = $isGov ? $ORG_GOV : $ORG_BUSINESS;
    $entityID = get_or_create_entity_org($dst, $dryRun, $orgMap, $company, $orgType, $display);
  } else {
    $addrKey = trim((string)($row['Address'] ?? '')) . '|' . trim((string)($row['PostalCode'] ?? ''));
    $entityID = get_or_create_entity_person($dst, $dryRun, $personMap, $first ?: 'Unknown', null, $last ?: 'Customer', $display, $addrKey);
  }

  if ($entityID <= 0 && !$dryRun) continue;

  assign_role($dst, $dryRun, $entityID, 'CUSTOMER');

  upsert_customer(
    $dst,
    $dryRun,
    $entityID,
    $legacyID,
    $row['InternalComments'] ?? null,
    $typeID !== '' ? $typeID : null,
    $row['CreditLim'] ?? null,
    $row['CheckApp'] ?? null,
    $row['ChargeApp'] ?? null,
    $row['DefaultPricing'] ?? null,
    $row['TaxExempt'] ?? 0,
    $row['ResaleNo'] ?? null
  );

  // addresses
  add_address($dst, $dryRun, $entityID, 'billing', $row['Address'] ?? null, null, $row['City'] ?? null, $row['State'] ?? null, $row['PostalCode'] ?? null, 'US', true);
  add_address($dst, $dryRun, $entityID, 'shipping', $row['SAddress'] ?? null, null, $row['SCity'] ?? null, $row['SState'] ?? null, $row['SPostalCode'] ?? null, 'US', false);

  // phones/emails
  add_phone($dst, $dryRun, $entityID, 'main', $row['PhoneNumber'] ?? null, true);
  add_email($dst, $dryRun, $entityID, 'main', $row['Email'] ?? ($row['email'] ?? null), true);

  $stats['customers']++;
}
$r->free();
logmsg("Customers migrated: {$stats['customers']}");

// -------------------------
// Vendors
// -------------------------
logmsg("tblVendors -> entities/vendor_accounts ...");
$r = q($src, "SELECT * FROM tblVendors");
while ($row = $r->fetch_assoc()) {
  $legacyID = isset($row['VendorID']) ? intval($row['VendorID']) : null;
  $company  = trim((string)($row['CompanyName'] ?? ''));
  $first    = trim((string)($row['FirstName'] ?? ''));
  $last     = trim((string)($row['LastName'] ?? ''));
  $contact  = trim((string)($row['ContactName'] ?? ''));

  $display = $company !== '' ? $company : ($contact !== '' ? $contact : (concat_name($first, $last) ?: ("Vendor $legacyID")));
  $entityID = -1;
  if ($company !== '') {
    // Map legacy LookupCode -> canonical vendor_lookup_codes (also used for org type heuristics)
    $legacyLookupRaw = trim((string)($row['LookupCode'] ?? ''));
    $lk = map_vendor_lookup($legacyLookupRaw);
    $vendorLookupId = $dryRun ? null : getVendorLookupCodeId($dst, $lk['code'], $lk['desc']);

    // Heuristic: treat Banking/Credit Cards as financial orgs
    $isFin = in_array($lk['code'], ['BANKING', 'CREDIT_CARDS'], true);
    $orgType = $isFin ? $ORG_FIN : $ORG_BUSINESS;
    $entityID = get_or_create_entity_org($dst, $dryRun, $orgMap, $company, $orgType, $display);
  } else {
    $addrKey = trim((string)($row['Address'] ?? '')) . '|' . trim((string)($row['Zip'] ?? ''));
    $entityID = get_or_create_entity_person($dst, $dryRun, $personMap, $first ?: 'Unknown', null, $last ?: 'Vendor', $display, $addrKey);
  }

  if ($entityID <= 0 && !$dryRun) continue;

  assign_role($dst, $dryRun, $entityID, 'VENDOR');

  upsert_vendor(
    $dst,
    $dryRun,
    $entityID,
    $legacyID,
    isset($row['PaytermsID']) ? intval($row['PaytermsID']) : null,
    isset($row['AcctID']) ? intval($row['AcctID']) : null,
    $vendorLookupId,
    $legacyLookupRaw,
    $row['FedTaxNo'] ?? null,
    $row['StateTaxNo'] ?? null,
    $row['CreditLimit'] ?? null,
    $row['Comments'] ?? null
  );

  add_address($dst, $dryRun, $entityID, 'main', $row['Address'] ?? null, null, $row['City'] ?? null, $row['State'] ?? null, $row['Zip'] ?? null, 'US', true);
  add_phone($dst, $dryRun, $entityID, 'main', $row['Phone'] ?? null, true);
  add_phone($dst, $dryRun, $entityID, 'fax', $row['Fax'] ?? null, false);
  add_email($dst, $dryRun, $entityID, 'main', $row['email'] ?? null, true);

  $stats['vendors']++;
}
$r->free();
logmsg("Vendors migrated: {$stats['vendors']}");

// -------------------------
// Employees
// -------------------------
logmsg("tblEmployees -> entities/employee_accounts ...");
$r = q($src, "SELECT * FROM tblEmployees");
while ($row = $r->fetch_assoc()) {
  $legacyIDRaw = trim((string)($row['EmployeeId'] ?? ''));
  $legacyID = ($legacyIDRaw !== '') ? $legacyIDRaw : null;
  $first    = trim((string)($row['Firstname'] ?? ''));
  $middle   = trim((string)($row['Middlename'] ?? ''));
  $last     = trim((string)($row['Lastname'] ?? ''));
  $display  = concat_name($first, $last) ?: ("Employee $legacyID");

  $addrKey = trim((string)($row['Addr1'] ?? '')) . '|' . trim((string)($row['Zip'] ?? ''));
  $entityID = get_or_create_entity_person($dst, $dryRun, $personMap, $first ?: 'Unknown', $middle !== '' ? $middle : null, $last ?: 'Employee', $display, $addrKey);

  if ($entityID <= 0 && !$dryRun) continue;

  assign_role($dst, $dryRun, $entityID, 'EMPLOYEE');

  upsert_employee(
    $dst,
    $dryRun,
    $entityID,
    $legacyID,
    isset($row['PayTypeID']) ? intval($row['PayTypeID']) : null,
    $row['Supervisor'] ?? null,
    $row['SearchKey'] ?? null,
    $row['SSNNo'] ?? null,
    $row['HireDate'] ?? null,
    $row['TerminationDate'] ?? null,
    $row['Pay'] ?? null,
    $row['CurrentStatus'] ?? null,
    $row['Comments'] ?? null,
    $row['Active'] ?? 1
  );

  add_address($dst, $dryRun, $entityID, 'home', $row['Addr1'] ?? null, $row['Addr2'] ?? null, $row['City'] ?? null, $row['State'] ?? null, $row['Zip'] ?? null, $row['country'] ?? 'US', true);
  add_phone($dst, $dryRun, $entityID, 'work', $row['OfficePhone'] ?? null, true);
  add_phone($dst, $dryRun, $entityID, 'home', $row['HomePhone'] ?? null, false);

  $stats['employees']++;
}
$r->free();
logmsg("Employees migrated: {$stats['employees']}");

// -------------------------
// Timeclock events
// -------------------------
logmsg("tblEmpTimeClock -> employee_time_events ...");
$timeStats = migrate_timeclock($src, $dst, $dryRun);
$stats['time_events'] = $timeStats['events'];
$stats['time_rows'] = $timeStats['rows'];
$stats['time_skipped_missing_employee'] = $timeStats['skipped_missing_employee'];
$stats['time_skipped_no_timestamp'] = $timeStats['skipped_no_timestamp'];
logmsg("Timeclock rows processed: {$timeStats['rows']} | events inserted: {$timeStats['events']} | missing employees: {$timeStats['skipped_missing_employee']} | missing timestamps: {$timeStats['skipped_no_timestamp']}");

// -------------------------
// Sublet providers (distinct names)
// -------------------------
logmsg("tblWOSublet distinct SubletCompany -> entities/sublet_accounts ...");
$r = q($src, "SELECT DISTINCT SubletCompany FROM tblWOSublet WHERE SubletCompany IS NOT NULL AND TRIM(SubletCompany) <> ''");
while ($row = $r->fetch_assoc()) {
  $name = trim((string)$row['SubletCompany']);
  if ($name === '') continue;

  $entityID = get_or_create_entity_org($dst, $dryRun, $orgMap, $name, $ORG_BUSINESS, $name);
  if ($entityID <= 0 && !$dryRun) continue;

  assign_role($dst, $dryRun, $entityID, 'SUBLET_PROVIDER');
  upsert_sublet_provider($dst, $dryRun, $entityID);

  $stats['sublet_companies']++;
}
$r->free();
logmsg("Sublet providers migrated: {$stats['sublet_companies']}");

logmsg("Migration complete.");
exit(0);
