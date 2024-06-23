import sqlite3

def transfer_dna_data(dnagedcom_db_path, rootsmagic_db_path):
    """
    Transfer DNA data from DNAGedcom database to RootsMagic database.

    Args:
    - dnagedcom_db_path (str): Path to the DNAGedcom SQLite database.
    - rootsmagic_db_path (str): Path to the RootsMagic SQLite database.

    Returns:
    - None
    """

    # Custom collation function for case-insensitive sorting
    def rmnocase_collation(x, y):
        return (x.lower() > y.lower()) - (x.lower() < y.lower())

    # Connect to the DNAGedcom database
    conn_dna = sqlite3.connect(dnagedcom_db_path)
    cursor_dna = conn_dna.cursor()

    # Connect to the RootsMagic database with custom collation
    conn_rm = sqlite3.connect(rootsmagic_db_path)
    conn_rm.create_collation("RMNOCASE", rmnocase_collation)
    cursor_rm = conn_rm.cursor()

    try:
        # Fetch DNA kit data from Ancestry_matchGroups in DNAGedcom
        cursor_dna.execute("""
            SELECT testGuid, matchGuid, matchTreeId, matchTestDisplayName,
                   ROUND(sharedCentimorgans, 2), sharedSegment, created_date, confidence, groupName
            FROM Ancestry_matchGroups
        """)
        ancestry_kits = cursor_dna.fetchall()

        # Fetch EventType from FactTypeTable in RootsMagic
        cursor_rm.execute("SELECT FactTypeID FROM FactTypeTable WHERE Name LIKE '%DNA%'")
        event_types = [event[0] for event in cursor_rm.fetchall()]

        # Fetch ethnic regions data from Ancestry_matchEthnicity in DNAGedcom
        cursor_dna.execute("SELECT matchGuid, ethnicregions FROM Ancestry_matchEthnicity")
        ethnicity_dict = {str(row[0]): str(row[1]) for row in cursor_dna.fetchall()}

        # Fetch the OwnerID for the testGuid
        cursor_rm.execute(f"""
            SELECT OwnerID FROM EventTable
            WHERE EventType IN ({','.join('?' * len(event_types))}) AND Details LIKE ?
        """, event_types + [f'%{ancestry_kits[0][0]}%'])
        test_result = cursor_rm.fetchone()

        if not test_result:
            print(f"No matching OwnerID found for testGuid: {ancestry_kits[0][0]}")
            return

        owner_id1 = str(test_result[0])

        # Process and insert data into RootsMagic database
        for kit in ancestry_kits:
            (
                testGuid, matchGuid, matchTreeId, matchTestDisplayName, sharedCentimorgans,
                sharedSegment, created_date, confidence, groupName
            ) = map(str, kit)

            # Check if DNA match exists in RootsMagic and retrieve OwnerID
            cursor_rm.execute(f"""
                SELECT OwnerID FROM EventTable
                WHERE EventType IN ({','.join('?' * len(event_types))}) AND Details LIKE ?
            """, event_types + [f'%{matchGuid}%'])
            match_result = cursor_rm.fetchone()

            if match_result:
                owner_id2 = str(match_result[0])
                ethnic_region = ethnicity_dict.get(matchGuid, "Unknown")
                note = f"{matchTestDisplayName} is {float(confidence):.2f}% of being {groupName}. Ethnic Regions: {ethnic_region}"

                # Calculate SharedPercent rounded to 2 decimal places
                if sharedCentimorgans:
                    sharedPercent = round((float(sharedCentimorgans) / 7400) * 100, 2)
                else:
                    sharedPercent = None  # Handle cases where sharedCentimorgans is None or empty

                # Check if the record already exists in DNATable
                cursor_rm.execute("""
                    SELECT 1 FROM DNATable
                    WHERE ID1 = ? AND ID2 = ?
                """, (owner_id1, owner_id2))
                existing_record = cursor_rm.fetchone()

                if existing_record:
                    print(f"Skipping duplicate record in DNATable for OwnerIDs {owner_id1} and {owner_id2}: {matchTestDisplayName}")
                else:
                    # Insert or replace data into DNATable
                    cursor_rm.execute("""
                        INSERT OR REPLACE INTO DNATable (
                            ID1, ID2, Label1, Label2, DNAProvider, SharedCM, SharedPercent, LargeSeg,
                            SharedSegs, Date, Relate1, Relate2, CommonAnc, CommonAncType, Verified, Note, UTCModDate
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        owner_id1, owner_id2, matchTreeId, matchTestDisplayName, '2', sharedCentimorgans, sharedPercent, None,
                        sharedSegment, created_date, None, None, None, None, None, note, None
                    ))
                    print(
                        f"INSERTED/REPLACED record in DNATable for OwnerIDs {owner_id1} and {owner_id2}: {matchTestDisplayName}"
                    )

                # Check if the OwnerID exists in NameTable
                cursor_rm.execute("SELECT 1 FROM NameTable WHERE OwnerID = ?", (owner_id2,))
                if cursor_rm.fetchone():
                    # Update the existing row
                    cursor_rm.execute(
                        "UPDATE NameTable SET Given = ? WHERE OwnerID = ?",
                        (matchTestDisplayName, owner_id2)
                    )
                    action = "UPDATED"
                else:
                    # Insert a new row
                    cursor_rm.execute(
                        "INSERT INTO NameTable (OwnerID, Given) VALUES (?, ?)",
                        (owner_id2, matchTestDisplayName)
                    )
                    action = "INSERTED"

                print(f"{action} record in NameTable for OwnerID {owner_id2}: {matchTestDisplayName}")

        # Commit changes
        conn_rm.commit()
        print("Data transfer completed successfully.")

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        conn_rm.rollback()  # Rollback changes on error

    finally:
        # Close connections
        conn_dna.close()
        conn_rm.close()

# Uncomment the lines below and comment out the input lines to use default database paths
# dnagedcom_db_path = "C:\\DNAGedCom.db"
# rootsmagic_db_path = "C:\\RootsMagic.rmtree"

dnagedcom_db_path = input("Enter the path to the DNAGedcom database: ")
rootsmagic_db_path = input("Enter the path to the RootsMagic database: ")

# Execute the data transfer
transfer_dna_data(dnagedcom_db_path, rootsmagic_db_path)
