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

function logmsg(string $msg): void {
    $ts = date('Y-m-d H:i:s');
    fwrite(STDOUT, "[$ts] $msg\n");
}

function usage(): void {
    global $argv;
    $me = basename($argv[0]);
    fwrite(STDOUT, "Usage:\n  php $me create\n  php $me drop\n  php $me migrate [--dry-run]\n");
    exit(1);
}

function db_connect(string $host, string $user, string $pass, int $port, string $db): mysqli {
    $m = new mysqli($host, $user, $pass, $db, $port);
    if ($m->connect_error) {
        throw new RuntimeException("DB connect error ($db): " . $m->connect_error);
    }
    $m->set_charset('utf8mb4');
    return $m;
}

function q(mysqli $db, string $sql): mysqli_result|bool {
    $res = $db->query($sql);
    if ($res === false) {
        throw new RuntimeException("SQL error: " . $db->error . "\nSQL: $sql");
    }
    return $res;
}

function normalize_key(string $s): string {
    $s = strtolower(trim($s));
    $s = preg_replace('/\s+/', ' ', $s);
    $s = preg_replace('/[^a-z0-9 \-\.&]/', '', $s);
    return trim($s);
}

function concat_name(string $first, string $last): string {
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
  LegacyEmployeeID INT NULL UNIQUE,
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

SET FOREIGN_KEY_CHECKS=1;
SQL;

$dropSql = <<<SQL
SET FOREIGN_KEY_CHECKS=0;
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
function ensure_seed_data(mysqli $dst, bool $dryRun): void {
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
}

function role_id(mysqli $dst, string $code): int {
    $stmt = $dst->prepare("SELECT RoleID FROM entity_roles WHERE Code=? LIMIT 1");
    $stmt->bind_param("s", $code);
    $stmt->execute();
    $stmt->bind_result($id);
    if ($stmt->fetch()) { $stmt->close(); return intval($id); }
    $stmt->close();
    throw new RuntimeException("Role not seeded: $code");
}

function org_type_id(mysqli $dst, string $code): int {
    $stmt = $dst->prepare("SELECT OrgTypeID FROM org_types WHERE Code=? LIMIT 1");
    $stmt->bind_param("s", $code);
    $stmt->execute();
    $stmt->bind_result($id);
    if ($stmt->fetch()) { $stmt->close(); return intval($id); }
    $stmt->close();
    throw new RuntimeException("Org type not seeded: $code");
}

function get_lookup_id(mysqli $dst, string $table, string $idCol, string $nameCol, string $value): int {
    $value = trim($value);
    $stmt = $dst->prepare("SELECT $idCol FROM $table WHERE $nameCol=? LIMIT 1");
    $stmt->bind_param("s", $value);
    $stmt->execute();
    $stmt->bind_result($id);
    if ($stmt->fetch()) { $stmt->close(); return intval($id); }
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
function norm_vendor_lookup(string $s): string {
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
function map_vendor_lookup(?string $legacy): array {
    $legacy = $legacy ?? '';
    $n = norm_vendor_lookup($legacy);

    // CanonicalCode => [Description, Synonyms...]
    $map = [
        'WRECKER'            => ['Wrecker', 'WRECKER'],
        'TOWING'             => ['Towing', 'TOWING'],
        'OFFICE_SUPPLIES'    => ['Office Supplies', 'OFFICE SUPPLIES', 'OFFICE'],
        'SHOP_SUPPLIES'      => ['Shop Supplies', 'OFFICE-SHOP SUPPLIES', 'OFFICE SHOP SUPPLIES', 'SUPPLIES', 'SHOP'],
        'PARTS'              => ['Parts', 'PART', 'PARTS', "PART'S", "PARTS'S", "PART'S", "PARTS'"],
        'BUS_PARTS'          => ['Bus Parts', 'BUS PARTS'],
        'TIRES'              => ['Tires', 'TIRES', 'TIRE', "TIRE'S", "TIRES'"],
        'LIGHTING'           => ['Lighting', 'LIGHTS', 'LIGHTING'],
        'HYDRAULIC_PARTS'    => ['Hydraulic Parts', 'HYD.PARTS', 'HYD PARTS', 'HYDRAULIC PARTS'],
        'ALTERNATOR_STARTER' => ['Alternator/Starter', 'ALTERNATOR', 'STARTER', 'ALTERNATOR STARTER'],
        'VEHICLE_BODY'       => ['Vehicle Body', 'BODY', 'VEHICLE BODY'],
        'WINDSHIELD_GLASS'   => ['Windshield/Glass', 'WINDSHIELD', 'GLASS', 'WINDSHIELD GLASS'],
        'GAS_FUEL'           => ['Gas/Fuel', 'GAS', 'FUEL', 'GAS FUEL'],
        'MEALS'              => ['Meals', 'MEALS'],
        'ENTERTAINMENT'      => ['Entertainment', 'ENTERTAINMENT'],
        'BANKING'            => ['Banking', 'BANK', 'BANKING'],
        'CREDIT_CARDS'       => ['Credit Cards', 'CREDIT CARDS', 'CREDIT CARD'],
        'TAXES'              => ['Taxes', 'TAXES', 'TAX'],
        'UTILITIES'          => ['Utilities', 'UTILITIES'],
        'PHONE'              => ['Phone', 'PHONE'],
        'RENT'               => ['Rent', 'RENT'],
        'MEDICAL'            => ['Medical', 'MEDICAL'],
        'OIL'                => ['Oil', 'OIL'],
        'METAL'              => ['Metal', 'METAL'],
        'PAINT'              => ['Paint', 'PAINT'],
        'TOOLS'              => ['Tools', 'TOOLS'],
        'COMPUTERS_IT'       => ['Computers / IT', 'COMPUTER', 'COMPUTERS', 'COMPUTERS IT', 'IT'],
        'GSE'                => ['GSE', 'GSE'],
        'SERVICES'           => ['Services', 'SERVICES', 'SERVICE'],
        'FREIGHT'            => ['Freight', 'FREIGHT'],
        'SHIPPING'           => ['Shipping', 'SHIPPING'],
        'FLOWERS'            => ['Flowers', 'FLOWERS'],
        'CHARITY'            => ['Charity', 'CHARITY'],
        'CLOTHING'           => ['Clothing', 'CLOTHING'],
    ];

    foreach ($map as $code => $def) {
        $desc = $def[0];
        $syns = array_slice($def, 1);
        foreach ($syns as $syn) {
            if ($n === norm_vendor_lookup($syn)) {
                return ['code' => $code, 'desc' => $desc, 'legacy_norm' => $n];
            }
        }
    }

    return ['code' => 'UNCLASSIFIED', 'desc' => 'Unclassified', 'legacy_norm' => $n];
}

function getVendorLookupCodeId(mysqli $dst, string $code, string $desc): int {
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


function getPhoneTypeId(mysqli $dst, string $name): int { return get_lookup_id($dst,'phone_types','PhoneTypeID','PhoneTypeName', strtolower(trim($name ?: 'main'))); }
function getEmailTypeId(mysqli $dst, string $name): int { return get_lookup_id($dst,'email_types','EmailTypeID','EmailTypeName', strtolower(trim($name ?: 'main'))); }
function getAddressTypeId(mysqli $dst, string $name): int { return get_lookup_id($dst,'address_types','AddressTypeID','AddressTypeName', strtolower(trim($name ?: 'main'))); }
function getCustomerTypeId(mysqli $dst, string $name): int { return get_lookup_id($dst,'customer_types','CustomerTypeID','CustomerTypeName', trim($name)); }

function getPricingPlanId(mysqli $dst, string $code): int {
    $code = trim($code);
    if ($code === '') $code = 'D';
    $stmt = $dst->prepare("SELECT PricingPlanID FROM pricing_plans WHERE Code=? LIMIT 1");
    $stmt->bind_param("s", $code);
    $stmt->execute();
    $stmt->bind_result($id);
    if ($stmt->fetch()) { $stmt->close(); return intval($id); }
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

function add_phone(mysqli $dst, bool $dryRun, int $entityID, string $type, ?string $num, bool $primary=false): void {
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

function add_email(mysqli $dst, bool $dryRun, int $entityID, string $type, ?string $email, bool $primary=false): void {
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

function add_address(mysqli $dst, bool $dryRun, int $entityID, string $type, ?string $a1, ?string $a2, ?string $city, ?string $state, ?string $postal, ?string $country='US', bool $primary=false): void {
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

function get_or_create_entity_person(mysqli $dst, bool $dryRun, array &$personMap, string $first, ?string $middle, string $last, string $displayName, ?string $addrKey): int {
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

function get_or_create_entity_org(mysqli $dst, bool $dryRun, array &$orgMap, string $legalName, int $orgTypeID, string $displayName): int {
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

function assign_role(mysqli $dst, bool $dryRun, int $entityID, string $roleCode): void {
    if ($dryRun) return;
    $rid = role_id($dst, $roleCode);
    $stmt = $dst->prepare("INSERT IGNORE INTO entity_role_map (EntityID, RoleID) VALUES (?,?)");
    $stmt->bind_param("ii", $entityID, $rid);
    $stmt->execute();
    $stmt->close();
}

function upsert_customer(mysqli $dst, bool $dryRun, int $entityID, ?int $legacyCustomerID, ?string $internalComments, ?string $typeName, $creditLim, $checkApp, $chargeApp, ?string $defaultPricing, $taxExempt, ?string $resaleNo): void {
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

function upsert_vendor(mysqli $dst, bool $dryRun, int $entityID, ?int $legacyVendorID, $paytermsID, $acctID, ?string $lookupCode, ?string $fedTaxNo, ?string $stateTaxNo, $creditLimit, ?string $comments): void {
    if ($dryRun) return;
    $cl = null;
    if ($creditLimit !== null && $creditLimit !== '' && is_numeric($creditLimit)) $cl = floatval($creditLimit);
    $stmt = $dst->prepare("INSERT IGNORE INTO vendor_accounts (EntityID, LegacyVendorID, PaytermsID, AcctID, VendorLookupCodeID, LegacyLookupCode, FedTaxNo, StateTaxNo, CreditLimit, Comments)
                           VALUES (?,?,?,?,?,?,?,?,?,?)");
    $stmt->bind_param("iiiiisssds", $entityID, $legacyVendorID, $paytermsID, $acctID, $vendorLookupCodeID, $legacyLookupCode, $fedTaxNo, $stateTaxNo, $cl, $comments);
    $stmt->execute();
    $stmt->close();
}

function getEmployeeStatusId(mysqli $dst, ?string $status): ?int {
    $status = trim((string)$status);
    if ($status === '') return null;
    return get_lookup_id($dst, 'employee_statuses', 'EmployeeStatusID', 'StatusName', $status);
}

function upsert_employee(mysqli $dst, bool $dryRun, int $entityID, ?int $legacyEmployeeID, $payTypeID, ?string $supervisor, ?string $searchKey, ?string $ssnNo, ?string $hireDate, ?string $termDate, $pay, ?string $currentStatus, ?string $comments, $active): void {
    if ($dryRun) return;
    $statusId = getEmployeeStatusId($dst, $currentStatus);
    $p = null;
    if ($pay !== null && $pay !== '' && is_numeric($pay)) $p = floatval($pay);
    $act = (intval($active) ? 1 : 0);

    $stmt = $dst->prepare("INSERT IGNORE INTO employee_accounts (EntityID, LegacyEmployeeID, PayTypeID, Supervisor, SearchKey, SSNNo, HireDate, TerminationDate, Pay, EmployeeStatusID, Comments, Active)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
    $stmt->bind_param("iiisssssdisi", $entityID, $legacyEmployeeID, $payTypeID, $supervisor, $searchKey, $ssnNo, $hireDate, $termDate, $p, $statusId, $comments, $act);
    $stmt->execute();
    $stmt->close();
}

function upsert_sublet_provider(mysqli $dst, bool $dryRun, int $entityID): void {
    if ($dryRun) return;
    $stmt = $dst->prepare("INSERT IGNORE INTO sublet_accounts (EntityID, Notes) VALUES (?,NULL)");
    $stmt->bind_param("i", $entityID);
    $stmt->execute();
    $stmt->close();
}

// -------------------------
// Main
// -------------------------
$cmd = $argv[1] ?? '';
$dryRun = in_array('--dry-run', $argv, true);

if (!in_array($cmd, ['create','drop','migrate'], true)) usage();

$src = db_connect($DB_HOST, $DB_USER, $DB_PASS, $DB_PORT, $SRC_DB);
$dst = db_connect($DB_HOST, $DB_USER, $DB_PASS, $DB_PORT, $DST_DB);

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

// migrate
logmsg("Migrating $SRC_DB -> $DST_DB (dryRun=" . ($dryRun ? 'true' : 'false') . ") ...");
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
    $display  = $company !== '' ? $company : (concat_name($first,$last) ?: ("Customer $legacyID"));

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
        $dst, $dryRun, $entityID,
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

    $display = $company !== '' ? $company : ($contact !== '' ? $contact : (concat_name($first,$last) ?: ("Vendor $legacyID")));
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
        $dst, $dryRun, $entityID,
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
    $legacyID = isset($row['EmployeeId']) ? intval($row['EmployeeId']) : null;
    $first    = trim((string)($row['Firstname'] ?? ''));
    $middle   = trim((string)($row['Middlename'] ?? ''));
    $last     = trim((string)($row['Lastname'] ?? ''));
    $display  = concat_name($first,$last) ?: ("Employee $legacyID");

    $addrKey = trim((string)($row['Addr1'] ?? '')) . '|' . trim((string)($row['Zip'] ?? ''));
    $entityID = get_or_create_entity_person($dst, $dryRun, $personMap, $first ?: 'Unknown', $middle !== '' ? $middle : null, $last ?: 'Employee', $display, $addrKey);

    if ($entityID <= 0 && !$dryRun) continue;

    assign_role($dst, $dryRun, $entityID, 'EMPLOYEE');

    upsert_employee(
        $dst, $dryRun, $entityID,
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
