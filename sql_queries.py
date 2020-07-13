fact_immigration_create = (
    """
    CREATE TABLE IF NOT EXISTS immigration(
        cicid STRING NOT NULL,
        arr_city_port_code TEXT,
        arr_date INT,
        arr_mode TEXT,
        arr_state_code TEXT,
        departure_date INT,
        file_date STRING,
        airline TEXT,
        flight_num TEXT,
        arrival_flag TEXT,
        departure_flag TEXT,
        update_flag TEXT,
        match_flag TEXT);
    """)

dim_immigrant_info_create = (
    """
    CREATE TABLE IF NOT EXISTS immigrant_info(
        cicid STRING NOT NULL,
        age INT,
        gender INT,
        bithyear INT,
        country_citizen_code TEXT,
        country_residence_code TEXT,
        visa_category TEXT,
        visa_issue_dep TEXT,
        visa_type TEXT,
        occupation TEXT,
        date_admitted TEXT,
        ins_num TEXT,
        adm_num TEXT);
    """)

dim_city_temperature_create = (
    """
    CREATE TABLE IF NOT EXISTS city_temperature(
        city TEXT NOT NULL,
        dt TEXT NOT NULL,
        average_temperature FLOAT,
        average_temperature_uncertainty FLOAT,
        latitude TEXT,
        longitude TEXT);
    """)

dim_city_demographic_create = (
    """
    CREATE TABLE IF NOT EXISTS city_demographic(
        city TEXT NOT NULL,
        state TEXT,
        median_age INT,
        male_population INT,
        female_population INT,
        total_population INT,
        veteran_num INT,
        foreign_born INT,
        average_household_size FLOAT,
        state_code TEXT,
        race TEXT,
        count INT);
    """)

dim_airport_code_create = (
    """
    CREATE TABLE IF NOT EXISTS airport_code(
        ident TEXT NOT NULL,
        type TEXT,
        name TEXT,
        elevation_ft FLOAT,
        iso_region TEXT,
        municipality TEXT NOT NULL,
        gps_code TEXT,
        iata_code TEXT,
        local_code TEXT,
        coordinates TEXT);
    """)