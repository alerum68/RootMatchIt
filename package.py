import os

files = [
    "setup_logging.py",
    "database.py",
    "rm-classes.py",
    "a_classes.py",
    "ftdna_classes.py",
    "mh_classes.py",
    "import_data.py",
    "process_data.py",
    "create_indices.py",
    "main.py"
]

with open("DG2RM.py", "w") as outfile:
    for fname in files:
        with open(fname) as infile:
            outfile.write(infile.read())
            outfile.write("\n\n")
