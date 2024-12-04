-- Table entities
CREATE TABLE IF NOT EXISTS entities (
    EntityID INTEGER PRIMARY KEY AUTOINCREMENT,
    "Name" TEXT NOT NULL,
    CCN TEXT NOT NULL UNIQUE,
    NPI TEXT NOT NULL UNIQUE,
    "Type" TEXT,
    Subtype TEXT,
    NUCC_Code TEXT,
    Unique_facility_at_location BOOLEAN DEFAULT 0,
    Employer_group_type TEXT DEFAULT 'none',
    Entity_unique_to_address BOOLEAN DEFAULT 1,
    Multi_speciality_facility BOOLEAN DEFAULT 0,
    Multi_speciality_employer BOOLEAN DEFAULT 0,
    Employer_num TEXT
);

-- Table addresses
CREATE TABLE IF NOT EXISTS addresses (
    Address_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    NPI TEXT,
    CCN TEXT,
    "Address" TEXT NOT NULL,
    City TEXT NOT NULL,
    State_ID INTEGER,
    Zip TEXT,
    Cms_addr_id TEXT,
    Address_Hash INTEGER NOT NULL,
    Primary_practice_address BOOLEAN DEFAULT 0,
    FOREIGN KEY (State_ID) REFERENCES states(State_ID),
    FOREIGN KEY (NPI) REFERENCES entities(NPI),
    FOREIGN KEY (CCN) REFERENCES entities(CCN)
);

-- Table states
CREATE TABLE IF NOT EXISTS states (
    State_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    StateCode TEXT NOT NULL UNIQUE,
    StateName TEXT NOT NULL
);

-- Table address_geolocation
CREATE TABLE IF NOT EXISTS address_geolocation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    Address_Hash INTEGER NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    FOREIGN KEY (Address_Hash) REFERENCES addresses(Address_Hash)
);

-- Indexes for optimization
CREATE INDEX IF NOT EXISTS idx_entities_npi_ccn ON entities (NPI, CCN);
CREATE INDEX IF NOT EXISTS idx_addresses_hash ON addresses (Address_Hash);
CREATE INDEX IF NOT EXISTS idx_states_code ON states (StateCode);