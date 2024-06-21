import sqlite3


def transfer_dna_data(dnagedcom_db_path, rootsmagic_db_path):
    # Connect to the DNAGedcom database
    conn_dna = sqlite3.connect(dnagedcom_db_path)
    cursor_dna = conn_dna.cursor()

    # Custom collation function for RMNOCASE
    def rmnocase_collation(x, y):
        if x.lower() < y.lower():
            return -1
        elif x.lower() > y.lower():
            return 1
        else:
            return 0

    # Connect to the RootsMagic database with custom collation
    conn_rm = sqlite3.connect(rootsmagic_db_path)
    conn_rm.create_collation("RMNOCASE", rmnocase_collation)
    cursor_rm = conn_rm.cursor()

    try:
        # Fetch DNA kit data from Ancestry_matchGroups in DNAGedcom
        cursor_dna.execute(
            "SELECT testGuid, matchGuid, matchTreeId, matchTestDisplayName, sharedCentimorgans, sharedSegment, created_date, confidence, groupName FROM Ancestry_matchGroups")
        ancestry_kits = cursor_dna.fetchall()

        # Fetch EventType from FactTypeTable in RootsMagic
        cursor_rm.execute("SELECT FactTypeID FROM FactTypeTable WHERE Name LIKE '%DNA%'")
        event_types = cursor_rm.fetchall()

        # Extract EventType IDs
        event_type_ids = [event[0] for event in event_types]

        # Fetch ethnic regions data from Ancestry_matchEthnicity in DNAGedcom
        cursor_dna.execute("SELECT matchGuid, ethnicregions FROM Ancestry_matchEthnicity")
        ancestry_ethnicity = cursor_dna.fetchall()

        # Create a dictionary for ethnicity data
        ethnicity_dict = {str(ethnicity[0]): str(ethnicity[1]).replace(",", "\n") for ethnicity in ancestry_ethnicity}

        # Process and insert data into RootsMagic database
        for kit in ancestry_kits:
            testGuid, matchGuid, matchTreeId, matchTestDisplayName, sharedCentimorgans, sharedSegment, created_date, confidence, groupName = map(
                str, kit)

            # Check if DNA kit has corresponding event records in RootsMagic
            cursor_rm.execute(
                "SELECT COUNT(*) FROM EventTable WHERE EventType IN ({}) AND (Details LIKE ? OR Details LIKE ?)".format(
                    ','.join('?' * len(event_type_ids))), event_type_ids + [f'%{testGuid}%', f'%{matchGuid}%'])
            count = cursor_rm.fetchone()[0]

            if count > 0:
                # Retrieve OwnerIDs for both testGuid and matchGuid
                cursor_rm.execute(
                    "SELECT OwnerID, Details FROM EventTable WHERE EventType IN ({}) AND (Details LIKE ? OR Details LIKE ?)".format(
                        ','.join('?' * len(event_type_ids))), event_type_ids + [f'%{testGuid}%', f'%{matchGuid}%'])
                results = cursor_rm.fetchall()

                owner_id1 = None
                owner_id2 = None

                for result in results:
                    owner_id = str(result[0])
                    details = result[1]

                    if details == testGuid:
                        owner_id1 = owner_id
                    elif details == matchGuid:
                        owner_id2 = owner_id

                # Check if both OwnerIDs were found
                if owner_id1 is not None and owner_id2 is not None:
                    ethnic_region = ethnicity_dict.get(matchGuid, "Unknown").replace("_", " ")

                    # Round confidence to 2 decimal places
                    rounded_confidence = round(float(confidence), 2)

                    # Construct note with formatted output
                    note = f"{rounded_confidence}% chance of {matchTestDisplayName} being a {groupName}.\n Ethnic Regions:\n{ethnic_region}\n"

                    # Insert data into DNATable using both OwnerIDs
                    cursor_rm.execute('''
                        INSERT INTO DNATable (
                            ID1, ID2, Label1, Label2, DNAProvider, 
                            SharedCM, SharedPercent, LargeSeg, SharedSegs, Date, 
                            Relate1, Relate2, CommonAnc, CommonAncType, Verified, Note, UTCModDate
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (owner_id1, owner_id2, matchTreeId, matchTestDisplayName, '2', sharedCentimorgans, None, None,
                          sharedSegment, created_date, None, None, None, None, None, note, None))

                    print(f"Parsed record: {matchTestDisplayName}: {testGuid}, {matchGuid}")

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


# Prompt for database locations
dnagedcom_db_path = input("Enter the path to the DNAGedcom database: ")
rootsmagic_db_path = input("Enter the path to the RootsMagic database: ")

# Execute the data transfer
transfer_dna_data(dnagedcom_db_path, rootsmagic_db_path)
