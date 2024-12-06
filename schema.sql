-- Table entities
CREATE TABLE IF NOT EXISTS entities (
    entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL,
    ccn TEXT NOT NULL UNIQUE,
    npi TEXT NOT NULL UNIQUE,
    "type" TEXT,
    subtype TEXT,
    nucc_code TEXT,
    unique_facility_at_location BOOLEAN DEFAULT 0,
    employer_group_type TEXT DEFAULT 'none',
    entity_unique_to_address BOOLEAN DEFAULT 1,
    multi_speciality_facility BOOLEAN DEFAULT 0,
    multi_speciality_employer BOOLEAN DEFAULT 0,
    employer_num TEXT
);

-- Table addresses
CREATE TABLE IF NOT EXISTS addresses (
    address_id INTEGER PRIMARY KEY AUTOINCREMENT,
    npi TEXT,
    ccn TEXT,
    "address" TEXT NOT NULL,
    city TEXT NOT NULL,
    state_id INTEGER,
    zip_code TEXT,
    cms_addr_id TEXT,
    address_hash INTEGER NOT NULL,
    primary_practice_address BOOLEAN DEFAULT 0,
    FOREIGN KEY (state_id) REFERENCES states(state_id),
    FOREIGN KEY (npi) REFERENCES entities(npi) ON DELETE CASCADE,
    FOREIGN KEY (ccn) REFERENCES entities(ccn) ON DELETE CASCADE
);

-- Table states
CREATE TABLE IF NOT EXISTS states (
    state_id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code TEXT NOT NULL UNIQUE,
    state_name TEXT NOT NULL
);

-- Table address_geolocation
CREATE TABLE IF NOT EXISTS address_geolocation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    address_hash INTEGER NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    FOREIGN KEY (address_hash) REFERENCES addresses(address_hash) ON DELETE CASCADE
);

-- Indexes for optimization
CREATE INDEX IF NOT EXISTS idx_entities_npi_ccn ON entities (npi, ccn);
CREATE INDEX IF NOT EXISTS idx_addresses_hash ON addresses (address_hash);
CREATE INDEX IF NOT EXISTS idx_states_code ON states (state_code);